--\connect stackoverflow
--\connect serverfault
--\connect unix
--\connect dba
--\connect askubuntu
\connect superuser

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
    revision_guid text,
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
    link_type_id integer
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


-- add indices
create index badges_user_id_idx on badges (user_id);

create index comments_post_id_idx on comments (post_id);
create index comments_user_id_idx on comments (user_id);

create index posts_parent_id_idx on posts (parent_id);
create index posts_accepted_answer_id_idx on posts (accepted_answer_id);
create index posts_owner_user_id_idx on posts (owner_user_id);

create index post_history_post_id_idx on post_history (post_id);
create index post_history_post_history_type_id_idx on post_history (post_history_type_id);
create index post_history_revision_guid_idx on post_history (revision_guid);
create index post_history_user_id_idx on post_history (user_id);

create index post_links_post_id_idx on post_links (post_id);
create index post_links_related_post_id_idex on post_links (related_post_id);

create index votes_post_id_idx on votes (post_id);
create index votes_user_id_idx on votes (user_id);


-- aggregations
drop table answers_with_question;
create table answers_with_question (
    id integer primary key,
    parent_id integer not null, -- only for answers
    score integer,
    view_count integer,
    body text,
    owner_user_id integer,
    last_activity_date timestamp,
    closed_date timestamp,
    title text,
    tags text,
    parent_score integer,
    parent_view_count integer,
    parent_body text,
    parent_owner_user_id integer,
    parent_last_activity_date timestamp,
    parent_closed_date timestamp,
    parent_title text,
    parent_tags text
);

insert into answers_with_question (
    select a.id, a.parent_id, a.score, a.view_count, a.body, a.owner_user_id, a.last_activity_date,
    a.closed_date, a.title, a.tags,
    q.score, q.view_count, q.body, q.owner_user_id, q.last_activity_date, q.closed_date, q.title, q.tags
    from posts as q join posts as a on (q.id=a.parent_id)
);


drop table tags;
create table tags (
    name text primary key,
    occurrences integer not null
);

insert into tags (
    select tag, count(*) from (
        select trim(replace(replace(tag, '>', ''), '<', '')) as tag from posts,
         unnest(string_to_array(tags, '><')) as t(tag)

    ) as tmp
    group by tag
);

-- convenience methods to dump and restore database (run from root directory of project)

-- to dump
--pg_dump -Fc --dbname=postgresql://postgres:password@127.0.0.1:5432/stackoverflow -t posts > stackoverflow_posts.dump

-- to restore
--pg_restore -Fc --dbname=postgresql://postgres:password@127.0.0.1:5432/stackoverflow -t posts stackoverflow_posts.dump

