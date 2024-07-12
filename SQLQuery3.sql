select * from dbo.netflix_raw order by title 

select * from dbo.netflix_raw where show_id='s5023'

create table [dbo].[netflix_raw](
[show_id] [varchar] (10) primary key,
[type] [varchar] (10) NULL,
--title data type is nchar because korean titles as well need to be accepted
[title] [nvarchar] (200) NULL,
[director] [varchar] (250) NULL,
[cast] [varchar] (1000) NULL,
[country] [varchar] (150) NULL,
[date_added] [varchar] (20) NULL,
[release_year] [int] NULL,
[rating] [varchar] (10) NULL,
[duration] [varchar] (10) NULL,
[listed_in] [varchar] (100) NULL,
[description] [varchar] (500) NULL
)

--REMOVE DUPLICATES
--checking if show_id is duplicate, its not duplicate thus making it primary key
select show_id,COUNT(show_id) from netflix_raw group by show_id having COUNT(show_id)>1

--Checking if there exists duplicate title and which titles are exactly duplicate
--IN operator is a shorthand for multiple OR conditions
select * from netflix_raw where upper(title) in (select upper(title) 
from netflix_raw group by title having count(*)>1) order by title

--for some cases the title has same name but movie as well as tvshow so that is ok.
--for the cases where title is same for one type, then need to remove that.
--so check title and type as well while removing the duplicates.
--check those title whose type and title both are repeated
--select inside subquery doesnt allow to use more than 2 column so using concat
select * from netflix_raw where concat(upper(title),type) in(select concat(upper(title),type)
from netflix_raw group by upper(title),type having count(*)>1 ) order by title

--Total there are 3 duplicates, need to remove them
with cte as (select *,ROW_NUMBER() over(partition by title, type order by show_id) as rn
from netflix_raw) select * from cte where rn=1

--Doing analysis on multiple values is difficult so seperating the values is must
--The below query returns a values which contains directors without comma after cross apply string_split(director,',')
--Creating new tables for listed_in, director, country, cast 
select show_id, trim(value) as genre into netflix_genre from netflix_raw cross apply string_split(listed_in,',')

--DATA TYPE CONVERSION FOR DATE_ADDED
with cte as (
select *, ROW_NUMBER() over(partition by title, type order by show_id)
as rn from netflix_raw) select show_id, type, title, cast(date_added as date)
as date_added, 
release_year, rating, duration, description from cte where rn=1


--While doing string split, the records whose country was null didn't get inserted in 
--netflix_country table.
--In netflix_country table populate all show_ids whose country was null with some value
select * from netflix_raw where country is null
select show_id, country from netflix_raw where country is null

--POPULATE MISSING VALUES IN COUNTRY, DURATION COLUMNS

--If we get to know a particular director is directing a movie in one country, 
--his other movies can also be assumed to be directed in the same country for ease.
--So get all combinations of director and country and use it as mapping
--for populating null values
--This query gives the list of director and its country of direction
select director, country from netflix_country nc inner join netflix_directors nd on
nc.show_id=nd.show_id group by director,country order by director

--Above query is used as subquery in the below query
insert into netflix_country
select show_id, m.country from netflix_raw nr 
inner join(select director, country from netflix_country nc inner join
netflix_directors nd on
nc.show_id=nd.show_id group by director,country) m on nr.director=m.director
where nr.country is null order by show_id

--Duration was null and in rating column duration is present
select * from netflix_raw where duration is null

--Where duration was null, populate it with rating data
with cte as  (select *, ROW_NUMBER() over(partition by title, type order by show_id)
as rn from netflix_raw)
select show_id, type, title, cast(date_added as date) as date_added, 
release_year, rating, case when duration is null then rating else duration end as
duration,description from cte where rn=1

--window is the set of rows related to the current row that are used in computations for this row.
--OVER() – Defines the window (set of rows) and indicates that this is a window function; without this clause, it’s not a window function.
--Along with OVER() avg(),min(),sum(), row_number()window functions ar used
--PARTITION BY – Divides the window into smaller groups called partitions (optional); if omitted, the whole result set is one partition.
--ROW_NUMBER assigns a number to each row in each partition(For each partition, row number is assigned sequentially eg-1 to 5 for partition1, 1-3 for partition2 and so on)
--There is a partition for title and type

--FINAL QUERY for inserting the data into netflix table
with cte as  (select *, ROW_NUMBER() over(partition by title, type order by show_id)
as rn from netflix_raw)
select show_id, type, title, cast(date_added as date) as date_added, 
release_year, rating, case when duration is null then rating else duration end as
duration,description into netflix from cte

select * from netflix_raw
select * from netflix
select * from netflix_cast
select * from netflix_country
select * from netflix_directors
select * from netflix_genre


--NETFLIX DATA ANALYSIS
--1.--For each director count no. of movies and tv shows created by them in seperate columns
--for directors who have created tv shows and movies both

--MAIN QUERY
--after getting which directors have distinct_type=2, make 2 seperate columns
--specifying count of exactly how many movies and tvshow
select nd.director, 
count(distinct case when n.type='Movie' then n.show_id end) as no_of_movies,
count(case when n.type='TV Show' then n.show_id end) as no_of_tv_show
from netflix n
inner join netflix_directors nd
on n.show_id=nd.show_id 
group by nd.director having COUNT(distinct n.type)>1

--SUB-QUERY
--to find which directors have distinct_type=2 (1.movie, 2.tv show)
select nd.director, COUNT(distinct n.type) as distinct_type from netflix n 
inner join netflix_directors nd on n.show_id=nd.show_id group by nd.director
having COUNT(distinct n.type)>1
order by distinct_type desc

--2.--Which country has highest number of comedy movies
--MAIN QUERY
select top 1 nc.country, count ( ng.show_id) as no_of_movies from netflix_genre ng 
inner join netflix_country nc on nc.show_id=ng.show_id
inner join netflix n on n.show_id=nc.show_id
where ng.genre='Comedies' and n.type='Movie' group by nc.country order by no_of_movies desc

select nc.country, count(distinct ng.show_id) as no_of_movies from netflix_genre ng 
inner join netflix_country nc on nc.show_id=ng.show_id
where genre='Comedies' group by nc.country

--3--For each year(as per date added in netflix), which director has maximum no. of movies released
--MAIN QUERY
with cte as (select nd.director, year(date_added) as date_year, count(n.show_id) as no_of_movies from 
netflix n inner join netflix_directors nd
on n.show_id=nd.show_id where n.type='Movie'
group by nd.director, year(date_added)
)
,cte2 as
(select * , ROW_NUMBER() over (partition by date_year
order by no_of_movies desc) as rn 
from cte 
)
select * from cte2 where rn=1


--SUB-QUERY
with cte as (select nd.director, year(date_added) as date_year, count(n.show_id) as no_of_movies from 
netflix n inner join netflix_directors nd
on n.show_id=nd.show_id where n.type='Movie'
group by nd.director, year(date_added)
)
(select * , ROW_NUMBER() over (partition by date_year
order by no_of_movies desc) as rn 
from cte 
)

--4-What is average duration of movies in each genre
--since duration is a varchar column, cast it as an integer
--MAIN QUERY
select avg(
cast(REPLACE(duration, ' min', '') as int))
as duration_int ,ng.genre
from netflix n inner join 
netflix_genre ng on n.show_id=ng.show_id where type='Movie' group by ng.genre

--5--find list of directors who have created horror and comedy movies both
--display director names along with number of comedy and horror movies directed by them
--MAIN QUERY

--used distinct to ensure that whatever duplicate copies are there for genre are ruled out
--for example for director='Ahmed Zein' duplicate entries are there for genre
select nd.director,
count(case when ng.genre='Comedies' then n.show_id end) as no_of_comedies,
count(case when ng.genre='Horror Movies' then n.show_id end) as no_of_horrors
from netflix_directors nd
inner join netflix n on 
nd.show_id=n.show_id 
inner join
netflix_genre ng
on n.show_id=ng.show_id
where n.type='Movie' and ng.genre in('Comedies','Horror Movies')
group by nd.director
having count(distinct ng.genre)=2 order by nd.director

