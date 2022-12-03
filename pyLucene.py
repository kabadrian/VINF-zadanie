
DATA_PATH = ""
INDEX_DIR = "Books.Index"

import os, sys, lucene, ast, json
import glob

from java.nio.file import Paths

from org.apache.lucene.search import IndexSearcher, TermQuery, MatchAllDocsQuery
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.index import (IndexWriter, IndexReader,
                                     DirectoryReader, Term,
                                     IndexWriterConfig)
from org.apache.lucene.document import Document, Field, TextField, StringField

from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser, QueryParserBase

from org.apache.lucene.analysis.standard import StandardAnalyzer

text_fields = ['common.topic.description']
available_fields = ['book.book.editions', 'id', 'book.written_work.author', 'common.topic.description', 'type.object.name', 'book.written_work.date_of_first_publication']

class SimpleIndexer(object):

    def index(self, indexDir, input_path):
        parsed_files = glob.glob(input_path)
        for file in parsed_files:
            fin = open(file, "rt")
            # create and open an index writer
            config = IndexWriterConfig(StandardAnalyzer())
            iw = IndexWriter(indexDir, config)
            for line in fin:
                book_dict = ast.literal_eval(line)
                doc = Document()

                for key, value in book_dict.items():
                    doc.add(TextField("id", key, TextField.Store.YES))
                    for (k, v) in value.items():
                        for item in v:
                            if k == 'book.book.editions':
                                res = item[1:-1].split(', ')
                                for e in res:
                                    if len(e.split(':', 1)) < 2:
                                        continue
                                    book_edition_key = e.split(':', 1)[0][1:-1]
                                    book_edition_value = e.split(':', 1)[1]
                                    for value_item in book_edition_value[1:-1].split(', '):
                                        doc.add(TextField(book_edition_key, value_item.strip('\''), TextField.Store.YES))
                            doc.add(TextField(k, item.strip('@en'), TextField.Store.YES))

                iw.addDocument(doc)
            fin.close()
            iw.close()

    index = classmethod(index)


class BookSearcher(object):

    def __init__(self, directory, input_path=None):
        self.directory = directory
        self.indexDir = FSDirectory.open(Paths.get(os.path.join(self.directory, INDEX_DIR)))
        self.input_path = input_path

    def createIndex(self):
        if self.input_path:
            SimpleIndexer.index(self.indexDir, self.input_path)
        else:
            SimpleIndexer.index(self.indexDir, "kabac/part-*")

    def search_field(self, field, search):
        directory = FSDirectory.open(Paths.get(INDEX_DIR))
        searcher = IndexSearcher(DirectoryReader.open(directory))
        analyzer = StandardAnalyzer()
        query = QueryParser(field, analyzer).parse(search)
        scoreDocs = searcher.search(query, 50).scoreDocs
        print("%s total matching documents." % len(scoreDocs))

        resultArray = []
        for scoreDoc in scoreDocs:
            doc = searcher.doc(scoreDoc.doc)
            book = dict()
            for field in available_fields:
                book[field] = [str(i) for i in doc.getValues(field)]
            resultArray.append(book)

        return len(scoreDocs), resultArray


if __name__ == "__main__":
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    book_searcher = BookSearcher('./')

    index_exist = os.path.exists('./' + INDEX_DIR)

    if index_exist:
        if len(os.listdir('./' + INDEX_DIR)) == 0:
            index_exist = False

    print("Index exist: ", index_exist)
    if not index_exist:
        book_searcher.createIndex()

    while True:
        operation = int(input(
            "Provide operation to be done: \n0 - exit \n1 - search one field \n2 - search multiple fields\n")
        )
        if operation == 0:
            break
        if operation == 1:
            field = input("Provide search field: ")
            query = input("Provide search query: ")
            print('searching ', query, ' in ', field)
            result = book_searcher.search_field(field, query)
            print('documents_found: ', result[0])
            for item in result[1]:
                print(json.dumps(item, indent=4))
                print()


