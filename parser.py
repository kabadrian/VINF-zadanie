import re
import json


class Parser:
    def __init__(self, file_path):
        self.file_path = file_path
        self.triples_array = []
        self.pattern = '(<http:\/\/rdf.freebase.com\/(ns\/)?)|(<http:\/\/www.w3.org\/[0-9]*\/[0-9]*\/[0-9]*-*)|(\t\.\n)'
        self.entity_dictionary = dict()

    def load_triples(self):
        with open(self.file_path) as f:
            file_content = f.readlines()

        for item in file_content:
            self.triples_array.append(tuple(re.sub(self.pattern, '', item).split('\t')))

    def parse_triples(self):
        old_id = None
        object_type_arr = []

        last_object = {}

        for (subj, obj, value) in self.triples_array:
            is_type = re.search('type\.object\.type', obj)
            subj = re.sub('(\">$)|(>$)', '', subj)
            obj = re.sub('(type\.\w+\.)|(>$)', '', obj)
            value = re.sub('(\">$)|(>$)', '', value)
            value = re.sub("\\\"((\d+-?)+)\\\".+XMLSchema.+", r"\1", value)

            if old_id != subj:
                for object_type in object_type_arr:
                    if object_type and re.search('book', object_type):
                        if object_type not in self.entity_dictionary:
                            self.entity_dictionary[object_type] = {}
                        self.entity_dictionary[object_type][old_id] = last_object
                object_type_arr = []
                last_object = dict()

            if is_type:
                object_type_arr.append(value)

            if obj not in last_object:
                last_object[obj] = [value]
            else:
                last_object[obj].append(value)
            old_id = subj

    def save_as_json_to_file(self, output_file_path):
        f = open(output_file_path, "w")
        f.write(json.dumps(self.entity_dictionary, indent=4))
        f.close()

    def run(self):
        self.load_triples()
        self.parse_triples()
        self.save_as_json_to_file('json-entities2.txt')
