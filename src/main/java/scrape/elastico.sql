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

