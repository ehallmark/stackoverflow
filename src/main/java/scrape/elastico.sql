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


drop table fatal_errors;
create table fatal_errors (
    post_id integer primary key references posts,
    occurrences integer not null default(1)
);

insert into fatal_errors (
    select id, count(*) from posts where parent_id is null and (lower(title) like any(ARRAY['%crash%', '%went down%', '%segfault%', '%terminated%', '%killed%', '%exit code%', '%downtime%', '%fatal%', '%failure%', '%failed%']) or lower(body) like any(array['%crash%', '%failure%', '%fatal%', '%critical error%'])) group by id
);

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

drop table exceptions;
create table exceptions (
    post_id integer not null references posts,
    error_code text not null,
    occurrences integer not null default(1),
    primary key(error_code, post_id)
);

insert into exceptions (
    select id as post_id,trim((regexp_matches(trim(regexp_replace(lower(title || body), '\s+', ' ', 'g')),'(\m\w+(error|exception|warning)\M)', 'g'))[1]) as error_code, count(*) from posts where parent_id is null group by post_id, error_code
);
