
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
    # class method for indexing documents
    def index(self, indexDir, input_path):
        # get list of parsed files
        parsed_files = glob.glob(input_path)
        for file in parsed_files:
            # open the file for reading
            fin = open(file, "rt")
            # create and open an index writer
            config = IndexWriterConfig(StandardAnalyzer())
            iw = IndexWriter(indexDir, config)
            # iterate through the lines in the file
            for line in fin:
                # convert the string representation of a dictionary to a dictionary
                book_dict = ast.literal_eval(line)
                # create a new document
                doc = Document()
                # iterate through the items in the dictionary
                for key, value in book_dict.items():
                    # add the "id" field to the document
                    doc.add(TextField("id", key, TextField.Store.YES))
                    # iterate through the items in the nested dictionary
                    for (k, v) in value.items():
                        # iterate through the items in the list
                        for item in v:
                            # if the key is "book.book.editions", we need to parse the value differently
                            if k == 'book.book.editions':
                                res = item[1:-1].split(', ')
                                for e in res:
                                    if len(e.split(':', 1)) < 2:
                                        continue
                                    # split line by ":" character, and take first element (id) without "" characters
                                    book_edition_key = e.split(':', 1)[0][1:-1]
                                    # second element in array after split represents value
                                    book_edition_value = e.split(':', 1)[1]
                                    # values are in array, take each value from array string separately and add it to document
                                    for value_item in book_edition_value[1:-1].split(', '):
                                        doc.add(TextField(book_edition_key, value_item.strip('\''), TextField.Store.YES))
                            doc.add(TextField(k, item.strip('@en'), TextField.Store.YES))

                # write document to index file
                iw.addDocument(doc)
            fin.close()
            iw.close()

    index = classmethod(index)


class BookSearcher(object):

    # Initialize a BookSearcher object
    def __init__(self, directory, input_path=None):
        # directory: the directory where the books to be searched are located
        # input_path: the location of the input file used to create the search index
        self.directory = directory
        self.indexDir = FSDirectory.open(Paths.get(os.path.join(self.directory, INDEX_DIR)))
        self.input_path = input_path

    # Create a search index for the books in the directory specified in __init__
    def create_index(self):
        if self.input_path:
            SimpleIndexer.index(self.indexDir, self.input_path)
        else:
            SimpleIndexer.index(self.indexDir, "kabac/part-*")

    # Search the created index for books that match a given search query
    # Returns a tuple containing the number of matching books and an array of matching books
    # Each book is represented as a dictionary containing the book's field values
    def search_field(self, field, search):
        # field: the field in the book to search (e.g. title, author, etc.)
        # search: the search query
        directory = FSDirectory.open(Paths.get(INDEX_DIR))
        searcher = IndexSearcher(DirectoryReader.open(directory))
        analyzer = StandardAnalyzer()
        query = QueryParser(field, analyzer).parse(search)
        scoreDocs = searcher.search(query, 50).scoreDocs
        print("%s total matching documents." % len(scoreDocs))

        # Array of books that match a given search query
        resultArray = []

        # for every matched document in scoreDocs
        for scoreDoc in scoreDocs:
            doc = searcher.doc(scoreDoc.doc)
            # create empty dict object
            book = dict()
            # add all fields from available fields array to dictionary object
            for field in available_fields:
                book[field] = [str(i) for i in doc.getValues(field)]
            # append book dictionary to array
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
    # check if index exists, if yes we can just load it, if no we have to create index

    if not index_exist:
        book_searcher.create_index()

    while True:
        operation = int(input(
            "Provide operation to be done: \n0 - exit \n1 - search one field\n")
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


