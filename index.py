import csv
import os
import fasttext
from elasticsearch import Elasticsearch, helpers
from time import time
from nltk.corpus import stopwords
import pandas as pd
import re
import numpy
from gensim.parsing.preprocessing import remove_stopwords

def drop_coulmns(source_file, result_file):
    if not os.path.exists(result_file) and os.path.exists(source_file):
        with open(source_file, "rt") as source:
            csv.field_size_limit(100000000)
            rdr = csv.reader(source)
            with open(result_file, "wt") as result:
                wtr = csv.writer(result)
                print("start droping ... ")
                for r in rdr:
                    del r[4]
                    wtr.writerow(r)
    else:
        raise Exception('source_file : {} not exists or result_file : {} exists'.format(
            source_file, result_file))


def delete_stop_words(source_file):
    t1 = time()
    index = 0
    if os.path.exists(source_file):
        with open(source_file, "rt") as source:
            csv.field_size_limit(100000000)
            rdr = csv.reader(source)
            with open('books_final.csv', "wt") as result:
                wtr = csv.writer(result)
                for r in rdr:
                    index += 1
                    print(index)
                    if len(r) == 4:
                        r[2] = re.sub('[^a-zA-Z]', ' ', str(r[2]))
                        r[2] = r[2].lower()
                        r[2] = remove_stopwords(r[2])

                        r[3] = re.sub('[^a-zA-Z]', ' ', str(r[3]))
                        r[3] = r[3].lower()
                        r[3] = remove_stopwords(r[3])
                        wtr.writerow(r)
    print("done delete stop words in  {} s".format(time()-t1))


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


def convert_to_vector(index):
    elastic_client = Elasticsearch()
    search_param = {
            "size":1000,
            "query": {
                "match_all": {}
            },
            "_source": {
                "includes": "abstract",
                # "includes": "body text"

            }}
    models = []
    files = []
    words = ['a', 'b', 'c', 'd']
    # for read data by pagination 
    for frm in range(0, 1):
        search_param['from'] = frm
        response = elastic_client.search(index=index, body=search_param)
        docs = response['hits']['hits']
        for doc in docs:
            f = open('temp.txt', 'w+')
            f.write(doc['_source']['abstract'])
            text = doc['_source']['abstract']
            # f.write(doc['_source']['body text'])
            f.close()
            try:
                model = fasttext.train_unsupervised('temp.txt', 'skipgram',minn=2, maxn=5, dim=300,  epoch=1, lr=0.5)
                models.append({"_id":doc['_id'], "model":model})
                files.append(" ".join(['__label__' + w for w in model.words[:10]]) + " " + text + '\n')
            # it is beacuse minimum length of document should be 5 word and some abstract are les than that
            except Exception as e:
                print(e)
    return files



  

def learn_test_model(files):

    train = ''.join(files[:int((len(files) * 0.8))])
    test = ''.join(files[-int((len(files) * 0.2)):])

    tr = open("train.txt", 'w')
    tr.write(train)
    tr.close()

    tt = open("test.txt", 'w')
    tt.write(test)
    tt.close()

    model = fasttext.train_supervised(input="train.txt", lr=0.5, epoch=25, wordNgrams=2, bucket=200000, dim=50, loss='ova')
    
    print(model.test('test.txt'))

if __name__ == '__main__':
    # drop_coulmns('en-books-dataset.csv', 'books.csv')
    # delete_existing_index('books')
    # delete_stop_words('books.csv')
    # csv_reader_index('books', 'books_final.csv')
    files = convert_to_vector('books')
    learn_test_model(files)
    print('done !')

