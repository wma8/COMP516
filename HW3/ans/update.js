db.movies.updateMany({rating: {$gte:4}}, {$set:{"summary":"great"}})
db.movies.updateMany({rating: {$gte:2, $lt:4}}, {$set:{"summary":"ok"}})
db.movies.updateMany({rating: {$lt:2}}, {$set:{"summary":"bad"}})