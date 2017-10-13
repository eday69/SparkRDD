# SparkRDD
Using Spark RDD examples




val books=sc.textFile("sparkrdd/books.csv");

case class Book(isbn: String, 
                publisher_id: Int,
                title: String,
                price: Float,
                discount: Float )


def parse(row: String): Book={

  val fields = row.split(",")

  val isbn: String = fields(0)
  val publisher_id: Int = fields(1).toInt
  val title: String = fields(2)
  val price: Float = fields(3).toFloat
  val discount: Float = fields(4).toFloat

  Book(isbn, publisher_id, title, price, discount) }


val booksParsed=books.map(parse)

val totalPrice=booksParsed.map(_.price).reduce((x,y) => x+y)
