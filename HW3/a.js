db.movies.find({release_date: /.*1995.*/, "genre.Animation": true}).count()
