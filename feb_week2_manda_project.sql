use ig_clone
show tables
-- 2 We want to reward the user who has been around the longest, Find the 5 oldest users

select *, datediff(current_date,created_at) as days from users
order by days desc
limit 5


-- 3 To target inactive users in an email ad campaign, find the users who have never posted a photo.
show tables
select * from photos
select * from users


select u.id , u.username,count(p.user_id)  from users u
 left join photos p
on u.id= p.user_id
group by u.id
having count(p.user_id) = 0

-- subquerry
select id ,username from users
where id NOT IN (select user_id from photos)



-- 4 Suppose you are running a contest to find out who got the most likes 
-- on a photo. Find out who won?
show tables
select * from likes
select * from users
select * from photos


with cte as (
select photo_id , count(*) from likes
group by photo_id
order by count(*) desc
limit 1
)
select u.id , u.username,cte.*  from users u
right join photos p
on u.id = p.user_id
join cte 
on p.id = cte.photo_id


-- 5 The investors want to know how many times does the average user post.
show tables
select * from users
select * from photos

with cte as ( with cte1 as(
select user_id , count(*) as user_posts from photos
group by user_id
)
 select u.id , u.username , ifnull(user_posts,0)  user_posts from users u
left join cte1 on u.id = cte1.user_id)

select avg(user_posts) as avg_userpost from cte

-- 2nd method
with cte as (
select u.id , u.username , count(p.user_id)as post 
from users u
left join photos p
on u.id = p.user_id
group by u.username
)
select avg(post) from cte


-- 6 A brand wants to know which hashtag to
-- use on a post, and find the top 5 most used hashtags.
show tables
select * from tags
select * from photo_tags
select * from photos

# top 5 most used hashtag
select pt.tag_id, t.tag_name , count(pt.photo_id) no_of_use from photo_tags pt
left join tags t
on t.id = pt.tag_id
group by pt.tag_id
order by no_of_use desc
limit 5

# which hashtag used on a post
select t.tag_name , pt.photo_id ,t.id from photo_tags pt
left join tags t
on pt.tag_id = t.id

# stored procedure
DELIMITER //
create procedure hashtag_details(photoid int)
BEGIN
select photo_id from photo_tags
where photo_id = photoid ;

select t.tag_name , pt.photo_id ,t.id from photo_tags pt
left join tags t
on pt.tag_id = t.id
where pt.photo_id = photoid ;
end //

call hashtag_details(1)



# 
-- 7 To find out if there are bots, find users who have liked every single photo on the site.
select * from photos
select * from likes
select * from users

SELECT u.id ,u.username 
, count(p.user_id) AS users_in_photos, count(l.user_id)AS users_in_likes,
(count(p.user_id) - count(l.user_id)) as bots
FROM photos p
INNER JOIN likes l
ON p.id = l.photo_id
 INNER JOIN users u
 on u.id = l.user_id
group by u.id
having users_in_likes = max(p.id)

-- 8 Find the users who have created instagramid in may and 
--  select top 5 newest joinees from it?

select *, monthname(created_at)
,datediff(current_date,created_at) as joinees from users
where monthname(created_at) = 'MAY'
order by joinees
limit 5

-- 9 Can you help me find the users whose name starts with c and ends
--  with any number and have posted the photos as well as liked the photos?
select * from photos
select * from likes
select * from users


select u.id , u.username ,count(p.id) as users_in_photos ,count(l.photo_id) as users_in_likes from users u
join photos p on u.id = p.user_id
join likes l on p.id = l.photo_id
where username regexp '^c' and username regexp '[0-9]$'
group by u.id
order by u.username

-- 10 Demonstrate the top 30 usernames to the company who have posted 
-- photos in the range of 3 to 5.
select * from users
select * from photos


select u.id , u.username ,count(p.id) as photos_posted from users u
join photos p on u.id = p.user_id
group by u.id
having count(p.id) between 3 and 5
order by photos_posted desc
limit 30


-- 