multiplex
=========

Code for multiplexing information from csv files.

A sample run:

python multiplex/csv_multiplex.py -f loadData_small.csv -m xml -s buyer -x invoice_image -d invoice_number

usage: csv_multiplex.py [-h] -f FILENAME -m MODE -s MULTIPLEX_ON [-i INPUTDIR]
                        [-o OUTPUTDIR] [-x FIELD_TO_FILESYSTEM]
                        [-e ELEMENT_NAME] [-d ID_FIELD]

optional arguments:
  -h, --help            show this help message and exit
  -f FILENAME, --filename FILENAME
                        name of file to be procressed
  -m MODE, --mode MODE  output mode ("xml" or "csv")
  -s MULTIPLEX_ON, --multiplex-on MULTIPLEX_ON
                        name of column to multiplex on
  -i INPUTDIR, --inputdir INPUTDIR
                        input directory
  -o OUTPUTDIR, --outputdir OUTPUTDIR
                        output directory
  -x FIELD_TO_FILESYSTEM, --field-to-filesystem FIELD_TO_FILESYSTEM
                        field to be written to filesystem (xml mode only)
  -e ELEMENT_NAME, --element-name ELEMENT_NAME
                        element name for each row of input (xml mode only)
  -d ID_FIELD, --id-field ID_FIELD
                        field to use as an id value for the row (xml mode
                        only)
