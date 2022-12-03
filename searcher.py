import re
import json


class Searcher:
    def __init__(self):
        self.file_path = 'json-entities2.txt'
        self.query = {
            "search": "book",
            # "book_name": "grade",
            "author_name": "Martha",
        }
        self.result = []
        self.books = None
        self.author_result_dict = None
        self.authors = None

    def search(self):
        # Opening JSON file
        f = open(self.file_path)

        # returns JSON object as
        # a dictionary
        data = json.load(f)

        self.authors = data.get('book.author')
        self.books = data.get('book.book')

        if self.query.get('book_name'):
            self.filter_by_book_name()

        if self.query.get('author_name'):
            self.filter_by_author()

        print(self.result)

    def filter_by_book_name(self):
        search_book_name = self.query.get('book_name')
        if not search_book_name:
            raise Exception("Book name was not specified")
        if not self.books:
            raise Exception("Books need to be loaded first")

        for (book_id, book_object) in self.books.items():
            book_name = self.get_english_string(book_object.get('name'))
            if book_name:
                if re.search(search_book_name, book_name):
                    book_object = self.get_book_info(book_id)
                    self.result.append({"book": book_object})

    def filter_by_author(self):
        self.author_result_dict = {}
        for(key, value) in self.authors.items():
            author_name = self.get_english_string(value.get('name'))
            if author_name:
                if re.search(self.query.get('author_name'), author_name):
                    self.author_result_dict[author_name] = value.get('book.author.works_written')
        print(self.author_result_dict)

        for (key, book_id_list) in self.author_result_dict.items():
            books_array = []

            if book_id_list:
                for book_id in book_id_list:
                    book_object = self.get_book_info(book_id)
                    if book_object:
                        books_array.append(book_object)
                self.result.append({"author": key, "books": books_array})

    def get_book_info(self, book_id):
        if not self.books:
            raise Exception("Books need to be loaded first")

        book = self.books.get(book_id)
        if not book:
            return None

        book_object = {
            "id": book_id,
            "name": self.get_english_string(book.get('name')),
            "description": self.get_english_string(book.get('common.topic.description')),
        }

        return book_object

    def get_english_string(self, string_array):
        if not string_array:
            return None
        # return string_array[0]

        for string in string_array:
            if re.search('(\\\"(.+)\\\"@en)', string):
                return re.sub('^\\\"(.+)\\\"@en$', r"\1", string)
        return None

    def run(self):
        return
