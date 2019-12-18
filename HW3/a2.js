db.movies.aggregate(
    [
        {$group: 
            {_id: {summary: "$summary"}
            , count: { $sum: 1}}
        },
        {$project: 
            {_id:0, count: "$count",  summary: "$_id.summary"
            }
        }, 
        {$sort: 
            {count:1}
        }
    ]
).toArray()