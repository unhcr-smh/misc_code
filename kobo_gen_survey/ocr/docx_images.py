import sys
import os
#import docx2txt

import argparse
import re
import xml.etree.ElementTree as ET
import zipfile

nsmap = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}


def process_args():
    parser = argparse.ArgumentParser(description='A pure python-based utility '
                                                 'to extract text and images '
                                                 'from docx files.')
    parser.add_argument("docx", help="path of the docx file")
    parser.add_argument('-i', '--img_dir', help='path of directory '
                                                'to extract images')

    args = parser.parse_args()

    if not os.path.exists(args.docx):
        print('File {} does not exist.'.format(args.docx))
        sys.exit(1)

    if args.img_dir is not None:
        if not os.path.exists(args.img_dir):
            try:
                os.makedirs(args.img_dir)
            except OSError:
                print("Unable to create img_dir {}".format(args.img_dir))
                sys.exit(1)
    return args


def qn(tag):
    """
    Stands for 'qualified name', a utility function to turn a namespace
    prefixed tag name into a Clark-notation qualified tag name for lxml. For
    example, ``qn('p:cSld')`` returns ``'{http://schemas.../main}cSld'``.
    Source: https://github.com/python-openxml/python-docx/
    """
    prefix, tagroot = tag.split(':')
    uri = nsmap[prefix]
    return '{{{}}}{}'.format(uri, tagroot)


def xml2text(xml):
    """
    A string representing the textual content of this run, with content
    child elements like ``<w:tab/>`` translated to their Python
    equivalent.
    Adapted from: https://github.com/python-openxml/python-docx/
    """
    text = u''
    root = ET.fromstring(xml)
    for child in root.iter():
        if child.tag == qn('w:t'):
            t_text = child.text
            text += t_text if t_text is not None else ''
        elif child.tag == qn('w:tab'):
            text += '\t'
        elif child.tag in (qn('w:br'), qn('w:cr')):
            text += '\n'
        elif child.tag == qn("w:p"):
            text += '\n\n'
    return text


def process(docx, prefix, img_dir=None):
    text = u''

    # unzip the docx in memory
    zipf = zipfile.ZipFile(docx)
    filelist = zipf.namelist()

    # get header text
    # there can be 3 header files in the zip
    header_xmls = 'word/header[0-9]*.xml'
    for fname in filelist:
        if re.match(header_xmls, fname):
            text += xml2text(zipf.read(fname))

    # get main text
    doc_xml = 'word/document.xml'
    text += xml2text(zipf.read(doc_xml))

    # get footer text
    # there can be 3 footer files in the zip
    footer_xmls = 'word/footer[0-9]*.xml'
    for fname in filelist:
        if re.match(footer_xmls, fname):
            text += xml2text(zipf.read(fname))

    if img_dir is not None:
        # extract images
        for fname in filelist:
            _, extension = os.path.splitext(fname)
            if extension in [".jpg", ".jpeg", ".png", ".bmp"]:
                print(fname, os.path.basename(fname))
                dst_fname = os.path.join(img_dir, prefix + os.path.basename(fname))
                with open(dst_fname, "wb") as dst_f:
                    dst_f.write(zipf.read(fname))

    zipf.close()
    return text.strip()



print ("The script path: %s  script file: %s" %(os.path.split(sys.argv[0])[0], sys.argv[0]))

text = process("E:\_UNHCR\CODE\kobo_gen_survey\download_survey_pics\output_controller\media_file=fifield%2Fattachments%2Fb0b534743414482c9addd247d3ca241e%2F7ed700f1-2a8f-43da-9d27-f67a2421d167%2FScreen_2-9_31_57.docx", 
                        os.path.split(sys.argv[0])[0] + '\\tmp') 

directory = 'E:\_UNHCR\CODE\kobo_gen_survey\download_survey_pics\output_controller'
files = os.listdir(directory)
index = 0
while index < len(files):
    filename = files[index]
    if filename.endswith('.docx'):
        prefix = 'x'+str(index)+'_'
        text = process(directory +'\\'+filename, prefix, 
                        os.path.split(sys.argv[0])[0] + '\\tmp') 
        print('XXXX',index, text)
    index += 1
    
    




# if __name__ == '__main__':
#     args = process_args()
#     text = process(args.docx, args.img_dir)
#     sys.stdout.write(text.encode('utf-8'))