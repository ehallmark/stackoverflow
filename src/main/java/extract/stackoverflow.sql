\connect stackoverflow

drop table questions cascade;
create table questions (
    id integer primary key,
    base_question_id integer not null,
    title text not null,
    name text not null,
    href text not null,
    closed boolean not null,
    status text,
    status_reason text,
    vote_count integer not null,
    text text not null,
    html text not null,
    accepted boolean not null,
    community_question boolean not null,
    user_name text,
    user_reputation integer,
    date date not null,
    last_editor_name text,
    last_editor_reputation integer,
    last_edit_date date,
    num_views integer not null,
    check (community_question OR user_name is not null)
);

drop table status_users;
create table status_users (
    question_id integer not null,-- references questions (id),
    user_name text not null,
    primary key(question_id, user_name)
);

drop table answers cascade;
create table answers (
    id integer primary key,
    question_id integer not null,-- references questions (id),
    index integer not null,
    vote_count integer not null,
    text text not null,
    html text not null,
    accepted boolean not null,
    community_question boolean,
    user_name text,
    user_reputation integer,
    date date not null,
    last_editor_name text,
    last_editor_reputation integer,
    last_edit_date date,
    check (community_question OR user_name is not null)
);

drop table question_links;
create table question_links (
    question_id integer not null,-- references questions (id),
    other_question_id integer not null,
    primary key (question_id, other_question_id)
);

drop table question_related;
create table question_related (
    question_id integer not null,-- references questions (id),
    other_question_id integer not null,
    primary key (question_id, other_question_id)
);


drop table question_comments;
create table question_comments (
    question_id integer not null,-- references questions (id),
    comment_id integer not null,
    text text not null,
    html text not null,
    date date not null,
    user_name text not null,
    user_reputation integer,
    primary key (question_id, comment_id)
);

drop table answer_comments;
create table answer_comments (
    answer_id integer not null,-- references answers (id),
    comment_id integer not null,
    text text not null,
    html text not null,
    date date not null,
    user_name text not null,
    user_reputation integer,
    primary key (answer_id, comment_id)
);
