from pyspark.sql import SparkSession

def map_to_key_value(line):
    color_mapping = {
        'BLK': 'BLACK',
        'BK': 'BLACK',
        'WH': 'WHITE',
        'GY': 'GRAY',
        'GRY': 'GRAY',
        'BLU': 'BLUE',
        'GREY': 'GRAY',
        'BL': 'BLACK',
        'RD': 'RED',
        'SL': 'SILVER',
        'WHT': 'WHITE',
        'GR': 'GRAY'
    }
    fields = line.split(',')
    try:
        vehicle_color = fields[33]
        vehicle_color = color_mapping.get(vehicle_color, vehicle_color).upper()

        if vehicle_color:
            return (vehicle_color, 1)
    except IndexError:
        pass

    return (None, 0)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ticket_location_analysis <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("TicketLocationAnalysis").getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    ticket_locations = lines.map(map_to_key_value) \
                            .filter(lambda x: x[0] is not None) \
                            .reduceByKey(lambda x, y: x + y) \
                            .sortBy(lambda x: x[1], ascending=False)

    ticket_locations.saveAsTextFile("hdfs://128.10.0.12:9000/ticket_locations/output")

    output = ticket_locations.take(10)
    for (color, count) in output:
        print("%s\t%i" % (color, count))

    spark.stop()
