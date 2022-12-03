import unittest, lucene, os, shutil
from test import BookSearcher, INDEX_DIR, SimpleIndexer

class TestStringMethods(unittest.TestCase):
    book_searcher = None

    def test_exact_field_match(self):
        book_searcher = BookSearcher('./', './kabac/part-00000')

        result = book_searcher.search_field('book.book_edition.publication_date', '\"2014-03-25\"')

        self.assertEqual(result[0], 2)

    def test_partial_field_match(self):
        book_searcher = BookSearcher('./', './kabac/part-00000')

        result = book_searcher.search_field('type.object.name', '\"the United States of America\"')

        self.assertEqual(result[0], 3)

    def test_search_by_author(self):
        book_searcher = BookSearcher('./', './kabac/part-00000')

        result = book_searcher.search_field('book.written_work.author', '\"United States Congress\"')

        self.assertEqual(result[0], 3)


if __name__ == '__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    book_searcher = BookSearcher('./', './kabac/part-00000')

    index_exist = os.path.exists('./' + INDEX_DIR)

    if index_exist:
        shutil.rmtree('./' + INDEX_DIR)

    book_searcher.createIndex()

    unittest.main()

    shutil.rmtree('./' + INDEX_DIR)