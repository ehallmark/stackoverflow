\connect elastico


drop table posts cascade;
create table posts (
    id integer primary key,
    title text,
    body text,
    category text not null,
    creation_date timestamp,
    user_id integer,
    username text,
    num_comments integer
);

drop table post_comments;
create table post_comments (
    id integer primary key,
    post_id integer not null references posts (id),
    body text,
    creation_date timestamp,
    user_id integer,
    username text
);


-- errors
select * from posts where lower(title) like any(array[ '%break%', '%broke%', '%went down%', '%shutdown%', '%crash%', '%fail%', '%error%', '%exception%', '%critical%', '%warning%', '%exit%', '%problem%', '%fatal%', '%kill%', '%term%', '%status%']) and category='Elasticsearch' and lower(title || body) like any(ARRAY['%crash%', '%fatal%', '%failure%', '%failed%']) and not title like '%?';

-- non-errors
select * from posts where not lower(title||body) like any(array['%fatal%','%crash%', '%error%', '%failure%', '%exception%', '%warning%']) and category='Elasticsearch';


drop table error_codes;
create table error_codes (
    post_id integer not null references posts,
    error_code text not null,
    occurrences integer not null default(1),
    primary key(error_code, post_id)
);
insert into error_codes (

select id as post_id, trim(regexp_replace((regexp_matches(trim(regexp_replace(lower(title || body), '\s+', ' ', 'g')),'(((error|status|code|errno)((:{0,} {1,})|=)((0[xX][0-9a-fA-F]+)|([0-9]{2,}))(( {1,})|(\?)|($)))|( ((0[xX][0-9a-fA-F]+)|([0-9]{2,})) {1,}(error|status|code)))', 'g'))[1],'(code|status|error|errno|:|=|\s+)', '', 'g')) as error_code, count(*) from posts where parent_id is null group by post_id, error_code

);


-- run this to count the top error codes
select error_code, count(*) from (select id as post_id, trim(regexp_replace((regexp_matches(trim(regexp_replace(lower(title || body), '\s+', ' ', 'g')),'(((error|status|code|errno)((:{0,} {1,})|=)((0[xX][0-9a-fA-F]+)|([0-9]{2,}))(( {1,})|(\?)|($)))|( ((0[xX][0-9a-fA-F]+)|([0-9]{2,})) {1,}(error|status|code)))', 'g'))[1],'(code|status|error|errno|:|=|\s+)', '', 'g')) as error_code from posts
where (lower(title|| body) ~ '(((error|status|code|errno)((:{0,} {1,})|=)((0[xX][0-9a-fA-F]+)|([0-9]{2,}))(( {1,})|(\?)|($)))|( ((0[xX][0-9a-fA-F]+)|([0-9]{2,})) {1,}(error|status|code)))')) as temp group by error_code order by count(*) desc limit 20;


select id as post_id,trim((regexp_matches(trim(regexp_replace(lower(title || body), '\s+', ' ', 'g')),'(\m\w+(error|exception|warning)\M)', 'g'))[1]) as error_code from posts;


select error_code, count(*) from (select id as post_id,trim((regexp_matches(trim(regexp_replace(lower(title || body), '\s+', ' ', 'g')),'(\m\w+(error|exception|warning)\M)', 'g'))[1]) as error_code from posts) as t
group by error_code order by count(*) desc limit 100;


select