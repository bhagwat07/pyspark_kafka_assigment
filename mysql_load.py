import get_all_variable as gav

table = None
def mysql_connection(spark,db_name):
    try:
        if db_name == 'advertiser':
            table = (spark.read.format('jdbc')
             .option('url',gav.mysql_url)
             .option('dbtable',gav.advertiser)
             .option('user',gav.username)
             .option('password',gav.mysql_passwd)
             .load()
             )

        elif db_name == 'adslot':
            table = (spark.read.format('jdbc')
                             .option('url', gav.mysql_url)
                             .option('dbtable', gav.adslot)
                             .option('user', gav.username)
                             .option('password', gav.mysql_passwd)
                             .load()
                             )
    except Exception as e:
        print('Error in the method - mysql_connection. Please check the Stack trace. ',str(e))
    else:
        print('mysql connection create succesfully.!')

    return table
