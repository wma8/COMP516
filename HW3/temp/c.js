db.movies.aggregate([
    {
        $addFields: {
            year: {$substr: ["$release_date", 7, 4]}
        }
    }, {
        $group: {
            _id: {summary: "$summary", year: "$year"}, count: {$sum: 1}
        }
    }, {$match: 
        {"_id.year": {$ne:""}}
    },  {
        $sort:{"-id.year":1,"_id.summary":-1}
    }, {
        $limit:100
    }, {
        $project:{_id:0, "count":"$count", "year":"$_id.year", "summary":"$_id.summary"}
    }
]).toArray()