## Indexer BE
## Author: JJanku December 22
from datetime import datetime
import tkinter as tk
import os, glob
from elasticsearch import Elasticsearch, helpers
import docx
import PyPDF2

if os.name == 'posix':
    slash = "/" # for Linux and macOS
else:
    slash = chr(92) # '\' for Windows

def current_path():
    return os.path.dirname(os.path.realpath( __file__ ))

def get_files_in_dir(self=current_path()):

    file_list = []

    if self[-1] != slash:
        self = self + slash

    for filename in glob.glob(self + '**/*',recursive=True):
        print(filename)
        if os.path.isfile(filename)  :
            file_list += [filename]

    return file_list

def get_data_from_text_file(file):

    data = []

    for line in open(file, encoding="utf8", errors='ignore'):

        data += [ str(line) ]

    return data

def get_text(filename):
    fullText = []
  
    reader = PyPDF2.PdfReader(filename)
    for page in reader.pages:
        fullText.append(page.extract_text())
    return fullText
    
def getText(filename):
    doc = docx.Document(filename)
    fullText = []
    for para in doc.paragraphs:
        fullText.append(para.text)

    return fullText

import re
import pathlib
from datetime import datetime


def yield_docs(all_files, textB: tk.Text):
    for _id, _file in enumerate(all_files):
        textB.insert(tk.END ,"\nIndexing : " + _file)
        textB.see(tk.END)
        file_name = _file[ _file.rfind(slash)+1:]
        file_creation_time = datetime.fromtimestamp(os.path.getctime(_file)).strftime('%Y-%m-%dT%H:%M:%S')
        file_type = pathlib.Path(_file).suffix

        tokens:list =_file.split('WebContent')
        if len(tokens) == 1:
            print("Count is 1")
            tokens = _file.split('UserContent')
        if len(tokens) == 1:
            continue
        whats_left:list = tokens[1].split('\\')
        whats_left_final:list = []
        for token in whats_left:
            if len(token) > 0:
             token = '/'+token
             whats_left_final.append(token)

        print(whats_left_final)
        final_url = ''.join(whats_left_final)
        print(final_url)

        
        try:    
              if file_name.lower().endswith(('.html' , '.txt' , '.php' , '.htm')) is True :
                 data = get_data_from_text_file( _file )
                 data = "".join( data )

                 doc_source = {
                    "file_name": file_name,
                    "data": data ,
                    "file_path":final_url,
                    "file_type":file_type,
                    "creation_time":file_creation_time
                }
              elif file_name.lower().endswith((".docx", ".doc")) is True :
                   pages = getText(_file)
                   for page in pages:
                        doc_source = {
                            "file_name": file_name,
                            "data": page,
                            "file_path":final_url,
                            "file_type":file_type,
                            "creation_time":file_creation_time
                        }
                        yield {
                        "_index": "chipster",
                        "_source": doc_source
                        }
              elif file_name.lower().endswith((".pdf")) is True :
                    print("Ends with pdf")
                    pages = get_text(_file)
                    for page in pages:
                        doc_source = {
                            "file_name": file_name,
                            "data": page,
                            "file_path":final_url,
                            "file_type":file_type,
                            "creation_time":file_creation_time
                        }
                        yield {
                        "_index": "chipster",
                        "_source": doc_source
                        }   
              else:
                    doc_source = {
                    "file_name": file_name,
                    "data": final_url,
                    "file_path":final_url,
                    "file_type":file_type,
                    "creation_time":file_creation_time
                }
        
                    yield {
                    "_index": "chipster",
                    #  "_type": "some_type",
                    # "_id": _id + 1, # number _id for each iteration
                    "_source": doc_source
                }  
             
        except Exception as err:
          print('\nError ',err)
          doc_source = {
                    "file_name": file_name,
                    "data": final_url,
                    "file_path":final_url,
                    "file_type":file_type,
                    "creation_time":file_creation_time
                }
          yield {
                    "_index": "chipster",
                    #  "_type": "some_type",
                    # "_id": _id + 1, # number _id for each iteration
                    "_source": doc_source
                } 

root= tk.Tk('Chipster')
root.title("Chipster Folder Indexer ")
root.resizable(False,False)

canvas1 = tk.Canvas(root, width=400, height=520, relief='raised')


label1 = tk.Label(root, text='Chipster Indexer', background="lightblue")
label1.config(font=('Serif', 14, "bold"))
canvas1.create_window(200, 25, window=label1)

label2 = tk.Label(root, text='Enter the path of the folder to be indexed:')
label2.config(font=('helvetica', 10))
canvas1.create_window(200, 100, window=label2)

entry1 = tk.Entry(root , width=40) 
canvas1.create_window(200, 140, window=entry1)

label3 = tk.Label(root, text='Enter the URL of the elasticsearch instance:')
label3.config(font=('helvetica', 10))
canvas1.create_window(200, 180, window=label3)

entry2 = tk.Entry(root,width=40) 
canvas1.create_window(200, 220, window=entry2)


text = tk.Text(root, height=10,width=40)
# text.config(state= tk.ENAB)
import threading



def index():
 
 print("INDEX Clicked")
 elk_url = entry2.get()
 if elk_url is None or not elk_url:
  client = Elasticsearch("http://localhost:9200")
 else: 
  client = Elasticsearch(elk_url)

 dir_to_index = entry1.get()
 print("Got ", dir_to_index , elk_url)

 if os.path.isdir(dir_to_index) is False:  
   all_files = get_files_in_dir('.')
 else: 
  all_files = get_files_in_dir(dir_to_index)
   
 text.insert(tk.END,"TEST" ) # "\n " + "TOTAL FILES:", len( all_files )
 try:
   resp = helpers.bulk(
       client,
       yield_docs( all_files,text )
   )
   text.insert (tk.END,"\nhelpers.bulk() RESPONSE:"+ str(resp))
   text.insert (tk.END,"RESPONSE TYPE:"+ str(type(resp)))
 except Exception as err:
   print("\nhelpers.bulk() ERROR:", str(err))
 text.see(tk.END)
 return


def start_combine_in_bg():
    threading.Thread(target=index).start()
import tkinter.ttk as ttk

style = ttk.Style()
style.theme_use('alt')
style.configure('TButton', background = 'red', foreground = 'white', width = 20, borderwidth=1, focusthickness=4, focuscolor='none' , font=('Sans serif', 12, "bold"))
style.map('TButton', background=[('active','indianred')])

button1 = ttk.Button(text='Index', command=start_combine_in_bg)
# button1.pack()
canvas1.create_window(200, 260, window=button1)


canvas1.create_window(200,380,window=text)
print("END")
canvas1.pack()
root.mainloop()


