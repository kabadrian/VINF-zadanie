from parser import Parser
from searcher import Searcher


entity_parser = Parser('freebase-head-10000000')

entity_parser.run()

searcher = Searcher()

searcher.search()

