use role accountadmin;
use database roark_demo;
use warehouse lab_wh;

use schema public;

describe table nbc_loc;
select * from google_places_reviews limit 10;

select review_rating , review_text, owner_answer  from google_places_reviews 
where owner_answer is not null ;

select count(query) from google_places_reviews;
show users;