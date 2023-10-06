-- 1
use mavenmovies
select * from actor
select first_name, last_name,
concat(first_name, ' ', last_name) as full_name,
length(replace(concat(first_name, ' ', last_name), ' ',''))
as name_length
from actor
limit 10

select first_name, last_name,
concat(first_name, ' ', last_name) as full_name,
length(first_name) + length(last_name)
as name_length
from actor
limit 10
 
 
 -- 2
select * from actor_award
select first_name , last_name ,awards ,
concat(first_name,' ',last_name) as full_name,
length(concat(first_name,last_name)) as len_of_names from actor_award
where awards like '%oscar%' 



-- 3
show tables
select * from actor
select * from film
select *  from film_actor

select concat(a.first_name,' ',a.last_name) as full_name from actor a
inner join film_actor fa
on a.actor_id = fa.actor_id
inner join film f
on f.film_id = fa.film_id
where title like'Frost Head'


-- 4
select * from actor
select * from film
select *  from film_actor

select f.title from actor a
inner join film_actor fa
on a.actor_id = fa.actor_id
inner join film f
on f.film_id = fa.film_id
where a.first_name = 'will'  and a.last_name = 'wilson'

-- 5

show tables
select * from film
select * from rental
select * from inventory


select f.title ,datediff(return_date,rental_date), monthname(rental_date),monthname(return_date) from film f
inner join inventory i
on f.film_id = i.film_id
inner join rental r
on i.inventory_id = r.inventory_id
where monthname(rental_date) = 'may' and monthname(return_date) = 'may'

-- 6
show tables
select * from film
select* from category
select * from film_category

select f.title, c.name from film f
inner join film_category fc
on f.film_id = fc.film_id
  inner join category c
on fc.category_id = c.category_id
where c.name = 'comedy'