import pandas as pd
import csv
import os
from elasticsearch import Elasticsearch, helpers
from time import time

def drop_coulmns(source_file, result_file):
    if not os.path.exists(result_file) and os.path.exists(source_file):
        with open(source_file,"rt") as source:
            csv.field_size_limit(100000000)
            rdr= csv.reader(source)
            with open(result_file,"wt") as result:
                wtr= csv.writer(result)
                print("start droping ... ")
                for r in rdr:
                    del r[4]
                    wtr.writerow(r)
    else:
        raise Exception('source_file : {} not exists or result_file : {} exists'.format(source_file, result_file))

def delete_existing_index(index):
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    try:
        es.indices.delete(index=index)
    except:
        pass

def csv_reader_index(index, file_name):
    t1 = time()
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    es.indices.create(index=index) 
    csv.field_size_limit(100000000)
    with open(file_name, 'r') as outfile:
        reader = csv.DictReader(outfile)
        print('start indexing files ...')
        helpers.bulk(es, reader, index="books", doc_type="type")
    print("done indexing in  {} s".format(time()-t1))


if __name__ == '__main__':
    # drop_coulmns('en-books-dataset.csv', 'books.csv')
    # delete_existing_index('books')
    csv_reader_index('books', 'books.csv')
    print('done !')

    