import argparse
import csv
import logging
import os
import sys
import xml.etree.ElementTree as ET

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=logging.INFO)

class CsvReader(object):
    def __init__(self, filename, dir=None):
        if dir is None:
            real_path = filename
        else:
            real_path = os.path.join(dir, filename)
        logging.info('Opening file: {}'.format(real_path))
        self._filehandle = open(real_path)
        self._csv_reader = csv.reader(self._filehandle, delimiter=',', quotechar='"')
        self._headers = self._csv_reader.next()
        logging.info('Headers are: {}'.format(self._headers))

    def __iter__(self):
        return self._csv_reader
    
    @property
    def headers(self):
        return self._headers
    
    def next(self):
        try:
            return self._csv_reader.next()
        except Exception as e:
            print e.message
            try:
                self._filehandle.close()
            except:
                pass
            raise

class MultiplexCsvData(object):
    def __init__(self, filename, dispatch_field, **kwargs):
        self.reader = CsvReader(filename, kwargs.get('input_dir'))
        self.output_dir = self.setup_output_dir(filename, kwargs.get('output_dir'))
        self.headers = self.reader.headers
        self.output_mapping = {}
        self.dispatch_value = self.get_dispatch_value(dispatch_field, self.headers)

    @staticmethod
    def setup_output_dir(filename, output_dir):
        if output_dir is not None:
            actual_output_dir = output_dir
        else:
            filename_pieces = filename.split('.')
            if len(filename_pieces) == 1:
                actual_output_dir = '-'.join([filename, 'output'])
            else:
                actual_output_dir = '.'.join(filename_pieces[0:-1])
        if not os.path.isdir(actual_output_dir):
            if not os.path.exists(actual_output_dir):
                os.mkdir(actual_output_dir)
            else:
                raise Exception('Path {} exists, but cannot be used. It is not a directory'.format(actual_output_dir))
        return actual_output_dir


    @staticmethod
    def get_dispatch_value(dispatch_field, headers):
        index = headers.index(dispatch_field)
        return lambda data: data[index]

    def create_output_path(self, dispatch_value):
        if self.output_dir is None:
            return dispatch_value
        else:
            return os.path.join(self.output_dir, dispatch_value)

    def process(self, data):
        dispatch_value = self.dispatch_value(data)
        writer = None
        try:
            _, writer = self.output_mapping[dispatch_value]
        except KeyError:
            f, writer = self.initialize_output(dispatch_value)
            self.output_mapping[dispatch_value] = f, writer
        self.write_row(writer, data)

    def run(self):
        items_processed = 0
        for data in self.reader:
            self.process(data)
            items_processed += 1
            if items_processed % 10000 == 0:
                logging.info('Processed {} rows.'.format(items_processed))
        for f,_ in self.output_mapping.itervalues():
            self.file_epilogue(f)
            f.close()

    def file_epilogue(self, f):
        ## override in subclass when extra behavior is required at the close of a file
        pass 

class MultiplexCsvDataToCsv(MultiplexCsvData):
    def create_writer(self, output_path):
        actual_output_path = '.'.join([output_path, 'csv'])
        logging.info('Creating output file (csv): {}'.format(actual_output_path))
        f = open(actual_output_path, 'w')
        csv_writer = csv.writer(f, delimiter=',', quotechar='"')
        return f, csv_writer

    def initialize_output(self, dispatch_value):
        output_path = self.create_output_path(dispatch_value)
        f, csv_writer = self.create_writer(output_path)
        csv_writer.writerow(self.headers)
        return f, csv_writer

    def write_row(self, writer, data):
        writer.writerow(data)

class MultiplexCsvDataToXml(MultiplexCsvData):
    def __init__(self, filename, dispatch_field, **kwargs):
        super(MultiplexCsvDataToXml, self).__init__(filename, dispatch_field, **kwargs)
        self.field_to_filesystem = kwargs.get('field_to_filesystem')
        logging.info('field_to_filesystem: {}'.format(self.field_to_filesystem))
        self.element_name = kwargs.get('element_name', 'element')
        self.id_field = kwargs.get('id_field')
        self.id_index = None if not self.id_field else self.headers.index(self.id_field)

    # Danger, danger Will Robinson!!!!!!
    # The naive way of doing this will likely cause problems.
    # Writing one file per record will cause too many files to be located in a single directory,
    # which will clobber you on many (if not most) platforms. Very bad karma.

    def create_nested_path(self, name):
        def chunks(l, n):
            return [''.join(l[i:i+n]) for i in range(0, len(l), n)]
        _, id = name.split('-')
        levels = chunks(list(id), 2)
        levels.insert(0, self.id_field)
        if self.output_dir:
            levels.insert(0, self.output_dir)
        directory = os.path.join(*levels)
        if os.path.exists(directory) and not os.path.isdir(directory):
            raise Exception('Path {} already exists but is not a directory'.format(directory))
        if not os.path.exists(directory):
            logging.debug('Creating invoice image directory: {}'.format(directory))
            os.makedirs(directory)
        return os.path.join(directory, name)

    def write_field_to_file(self, name, value):
        actual_output_path = self.create_nested_path(name)
        f = open(actual_output_path + '.base64', 'w')
        f.write(value)
        f.close() 

    def create_writer(self, output_path):
        actual_output_path = '.'.join([output_path, 'xml'])
        logging.debug('Creating output file (xml): {}'.format(actual_output_path))
        f = open(actual_output_path, 'w')
        def writer(data):
            element = ET.Element(self.element_name)
            for k, v in zip(self.headers, data):
                if k == self.field_to_filesystem:
                    self.write_field_to_file(data[self.id_index], v)
                else:
                    sub = ET.SubElement(element, k)
                    sub.text = v
            f.write(ET.tostring(element))
            f.write('\n')
        return f, writer

    def initialize_output(self, dispatch_value):
        output_path = self.create_output_path(dispatch_value)
        f, writer = self.create_writer(output_path)
        f.write('<?xml version="1.0"?>\n')
        f.write('<invoices>\n')
        return f, writer

    def write_row(self, writer, data):
        writer(data)

    def file_epilogue(self, f):
        f.write('</invoices>\n')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--filename', help='name of file to be procressed', required=True)
    parser.add_argument('-m', '--mode', help='output mode ("xml" or "csv")', required=True, default='csv')
    parser.add_argument('-s', '--multiplex-on', help='name of column to multiplex on', required=True)
    parser.add_argument('-i', '--inputdir', help='input directory', required=False)
    parser.add_argument('-o', '--outputdir', help='output directory', required=False)
    parser.add_argument('-x', '--field-to-filesystem', help='field to be written to filesystem (xml mode only)', required=False)
    parser.add_argument('-e', '--element-name', help='element name for each row of input (xml mode only)', required=False)
    parser.add_argument('-d', '--id-field', help='field to use as an id value for the row (xml mode only)', required=False)
    return vars(parser.parse_args())

def verify_args(args):
    valid_modes = ['xml', 'csv']
    if args.get('mode') not in valid_modes:
        print 'Invalid mode (-m) = {}. Must be one of {}. Exiting'.format(args.get('mode'), valid_modes)
        sys.exit(1)
    if args.get('field_to_filesystem') and args.get('mode') != 'xml':
        print 'Invalid argument. Specifying "field-to-filesystem" (-x) only valid in "xml" mode. Exiting.'
        sys.exit(1)
    if args.get('id_field') and args.get('mode') != 'xml':
        print 'Invalid argument. Specifying "id-field" (-d) only valid in "xml" mode. Exiting.'
        sys.exit(1)
    if args.get('element_name') and args.get('mode') != 'xml':
        print 'Invalid combination. Specifying "element-name only valid in "xml" mode. Exiting.'
        sys.exit(1)

def main():
    args = parse_args()
    verify_args(args)
    logging.info('args:')
    for k, v in args.iteritems():
        logging.info('   {}: {}'.format(k, v))
    if args.get('mode') == 'csv':
        multi = MultiplexCsvDataToCsv(args['filename'], args['multiplex_on'])
    else:
        multi = MultiplexCsvDataToXml(
            args['filename'], 
            args['multiplex_on'], 
            field_to_filesystem=args.get('field_to_filesystem'), 
            id_field=args.get('id_field'),
            element_name=args.get('element_name')
            )
    multi.run()

if __name__ == '__main__':
    main()

