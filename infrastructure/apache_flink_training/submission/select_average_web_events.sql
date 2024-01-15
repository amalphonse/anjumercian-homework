--Compare the average values between zachwilson.techcreator.io, zachwilson.tech, and lulu.techcreator.io (`select_average_web_events`)


    select host, avg(num_hits) as avg_zach_host
    from insert_average_web_events where host="zachwilson.techcreator.io"


select host, avg(num_hits) as avg_zach_host
    from insert_average_web_events where host="zachwilson.tech"

select host, avg(num_hits) as avg_zach_host
    from insert_average_web_events where host="lulu.techcreator.io"
