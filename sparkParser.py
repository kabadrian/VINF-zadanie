from pyspark.sql import SparkSession
from pyspark.sql.types import *
import re
import sys, getopt
from pyspark.sql.functions import collect_list
from datetime import datetime

# spark-submit sparkParser.py -i <input_file_path> -o <output_file_path> -m <cluster_master> -s <spark_path_cluster>

def main(argv):
    book_regex = "^book\.book$"
    book_editions_regex = "^book\.book\.editions$"
    book_edition_regex = "book\.book_edition"
    book_author_regex = "book\.written_work\.author"
    book_author = "book\.author"
    book_genre_regex = "book\.book\.genre"
    name_regex = "type\.object\.name"
    year_of_publication = "book.written_work.date_of_first_publication"
    description_regex = "common.topic.description"
    isbn_regex = "media_common.cataloged_instance.isbn13"
    publication_date_regex = "book.book_edition.publication_date"
    en_string = "@en$"

    remove_pattern = '(http\:\/\/rdf.freebase.com\/ns\/)|(\^\^.*)|(\@.*\.)|(\<)|(\>)|(\")|(\t\.)'
    input_file = "./freebase-head-10000000"
    output_file = "./10m-outputs"
    master = 'local[*]'
    spark_url = None

    try:
        opts, args = getopt.getopt(argv, "hi:o:m:s", ["ifile=", "ofile=", "master=", "spark="])
    except getopt.GetoptError:
        print('Invalid arguments, if you need help type \'sparkParser.py -h\' ')
        sys.exit(1)
    for opt, arg in opts:
        if opt == '-h':
            print('sparkParser.py -i <inputfile> -o <outputfile> -m <master> -s <spark_url>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_file = arg
        elif opt in ("-o", "--ofile"):
            output_file = arg
        elif opt in ("-m", "--master"):
            master = arg
        elif opt in ("-s", "--spark"):
            spark_url = arg
    if spark_url:
        spark = SparkSession.builder.master(master).appName('VINF').getOrCreate().config('spark.executor.uri', spark_url)
    else:
        spark = SparkSession.builder.master(master).appName('VINF').getOrCreate()

    rdd = spark.sparkContext.textFile(input_file)

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Start splitting dataset: ", current_time)

    # split line to triplets and remove unnecesary characters (url addresses)
    all_rdd = rdd.map(lambda x: re.sub('\t\.', '', x)) \
        .map(lambda x: re.sub(remove_pattern, '', x)) \
        .map(lambda x: x.split('\t')) \
        .cache()

    # schema for rdd
    schema = StructType([StructField('subject', StringType(), False),
                    StructField('predicate', StringType(), False),
                    StructField('object', StringType(), False)])

    # filter book edition ids, and column that we need for book editions
    book_editions_rdd = all_rdd.filter(lambda x: re.search(book_edition_regex, x[2]))
    book_edition_columns = all_rdd.filter(lambda x: (re.search(name_regex, x[1]) and re.search(en_string, x[2])) or re.search(isbn_regex, x[1]) or re.search(publication_date_regex, x[1]))
    book_editions_id_df = spark.createDataFrame(book_editions_rdd, schema=schema).select('subject').distinct()
    book_editions_id_df = book_editions_id_df.dropDuplicates()
    book_edition_columns_df = spark.createDataFrame(book_edition_columns, schema=schema)
    book_editions_id_df.createOrReplaceTempView("book_editions_id")
    book_edition_columns_df.createOrReplaceTempView("book_editions_columns")


    books_editions_dataframe = spark.sql("""
        SELECT book_editions_id.subject, book_editions_columns.predicate, book_editions_columns.object
        from book_editions_id
        LEFT JOIN book_editions_columns on book_editions_id.subject == book_editions_columns.subject
        ORDER BY book_editions_id.subject
    """)

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Book editions dataframe: ", current_time)

    books_editions_dataframe.show(truncate=False)
    book_editions_grouped = books_editions_dataframe.groupBy(['subject', 'predicate']).agg(collect_list('object').alias('object'))

    book_editions_rdd = book_editions_grouped.rdd
    book_editions_rdd = book_editions_rdd.map(lambda x: (x[0], (x[1], x[2])))
    book_editions_rdd = book_editions_rdd.groupByKey().map(lambda x: (x[0], { '\''+str(name)+'\'' + ':' + str(value) for name, value in list(x[1])}))

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Book editions groupby: ", current_time)
    #
    schema_id = StructType([
        StructField("id", StringType(), False),
        StructField("properties", StringType(), True)
      ])

    book_editions_df = book_editions_rdd.toDF(schema_id)

    # book_editions_df = spark.createDataFrame(data=book_editions_dictionary, schema = schema_id)
    book_editions_df.createOrReplaceTempView("book_editions_json")

    # authors, genres

    author_columns = all_rdd.filter(lambda x: (re.search(name_regex, x[1]) and re.search(en_string, x[2])))
    author_genres_ids_rdd = all_rdd.filter(lambda x: re.search(book_author, x[2]) or re.search(book_genre_regex, x[2]))
    author_genres_book_df = spark.createDataFrame(author_genres_ids_rdd, schema = schema).select('subject').distinct()
    author_df = spark.createDataFrame(author_columns, schema = schema)
    author_df = author_df.dropDuplicates()
    author_df.createOrReplaceTempView("AUTHOR_COLUMNS")
    author_genres_book_df.createOrReplaceTempView("AUTHOR_DATA")

    author_dataframe = spark.sql("""
        SELECT AUTHOR_DATA.subject, AUTHOR_COLUMNS.predicate, AUTHOR_COLUMNS.object
        from AUTHOR_DATA
        LEFT JOIN AUTHOR_COLUMNS on AUTHOR_DATA.subject == AUTHOR_COLUMNS.subject
        ORDER BY AUTHOR_DATA.subject
    """)

    authors_grouped = author_dataframe.groupBy(['subject']).agg(collect_list('object').alias('object'))
    authors_grouped.createOrReplaceTempView("AUTHOR_GENRE_NAMES")

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Authors group by ended: ", current_time)

    # book

    book_ids_rdd = all_rdd.filter(lambda x: re.search(book_regex, x[2]))
    book_columns = all_rdd.filter(lambda x: re.search(book_editions_regex, x[1]) or re.search(book_author_regex, x[1]) or re.search(book_genre_regex, x[1]) or (re.search(name_regex, x[1]) and re.search(en_string, x[2])) or re.search(year_of_publication, x[1]) or (re.search(description_regex, x[1]) and re.search(en_string, x[2])))
    book_id_df = spark.createDataFrame(book_ids_rdd, schema = schema).select('subject').distinct()
    book_df = spark.createDataFrame(book_columns, schema = schema)
    book_df = book_df.dropDuplicates()
    book_df.createOrReplaceTempView("BOOK_COLUMNS")
    book_id_df.createOrReplaceTempView("BOOK_ID")

    books_dataframe = spark.sql("""
        SELECT BOOK_ID.subject, BOOK_COLUMNS.predicate, BOOK_COLUMNS.object
        from BOOK_ID
        JOIN BOOK_COLUMNS on BOOK_ID.subject == BOOK_COLUMNS.subject
        ORDER BY BOOK_ID.subject
    """)

    books_dataframe.createOrReplaceTempView("BOOK_DATA")
    books_dataframe = spark.sql("""
        SELECT BOOK_DATA.subject, BOOK_DATA.predicate, 
        CASE 
            when AUTHOR_GENRE_NAMES.subject is not null then (cast(AUTHOR_GENRE_NAMES.object as STRING))
            when book_editions_json.id is not null then book_editions_json.properties
            when AUTHOR_GENRE_NAMES.subject is null and book_editions_json.id is null then BOOK_DATA.object
        end as object
        from BOOK_DATA
        LEFT JOIN AUTHOR_GENRE_NAMES on BOOK_DATA.object == AUTHOR_GENRE_NAMES.subject
        LEFT JOIN book_editions_json on BOOK_DATA.object == book_editions_json.id
        ORDER BY BOOK_DATA.subject
    """)

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("books group by started: ", current_time)

    books_grouped = books_dataframe.groupBy(['subject', 'predicate']).agg(collect_list('object').alias('object'))
    grouped_rdd = books_grouped.rdd
    grouped_rdd = grouped_rdd.map(lambda x: (x[0], (x[1], x[2])))
    grouped_rdd = grouped_rdd.groupByKey().map(lambda x: {x[0]: {name: value for name, value in list(x[1])}})

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("books group by ended: ", current_time)

    grouped_rdd.saveAsTextFile(output_file)


if __name__ == "__main__":
   main(sys.argv[1:])