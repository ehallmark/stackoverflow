\connect stackoverflow

drop table badges;
create table badges (
    id integer primary key,
    user_id integer,
    name text,
    date timestamp
);


drop table comments;
create table comments (
    id integer primary key,
    post_id integer,
    score integer,
    text text,
    creation_date timestamp,
    user_id integer
);


drop table posts;
create table posts (
    id integer primary key,
    post_type_id integer,
    parent_id integer, -- only for answers
    accepted_answer_id integer,
    creation_date timestamp,
    score integer,
    view_count integer,
    body text,
    owner_user_id integer,
    last_editor_user_id integer,
    last_editor_display_name text,
    last_edit_date timestamp,
    last_activity_date timestamp,
    closed_date timestamp,
    title text,
    tags text,
    answer_count integer,
    comment_count integer,
    favorite_count integer
);


drop table post_history;
create table post_history (
    id integer primary key,
    post_history_type_id integer,
    post_id integer,
    revision_guid integer,
    creation_date timestamp,
    user_id integer,
    user_display_name text,
    comment text,
    text text,
    close_reason_id integer
);


drop table post_links;
create table post_links (
    id integer primary key,
    creation_date timestamp,
    post_id integer,
    related_post_id integer,
    post_link_type_id integer
);


drop table users;
create table users (
    id integer primary key,
    reputation integer,
    creation_date timestamp,
    display_name text,
    email_hash text,
    last_access_date timestamp,
    website_url text,
    location text,
    age integer,
    about_me text,
    views integer,
    up_votes integer,
    down_votes integer
);


drop table votes;
create table votes (
    id integer primary key,
    post_id integer,
    vote_type_id integer,
    creation_date timestamp,
    user_id integer,
    bounty_amount integer
);