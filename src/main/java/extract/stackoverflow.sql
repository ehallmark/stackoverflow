\connect stackoverflow

create table questions (
    id integer primary key,
    name text not null,
    href text not null,
    closed boolean not null,
    status text,
    status_reason text,
    vote_count integer not null,
    text text not null,
    html text not null,
    accepted boolean not null,
    answer_date date not null,
    community_question boolean not null,
    user text not null,
    user_reputation integer not null,
    num_views integer not null
);

create table status_users (
    question_id integer not null references questions (id),
    status_user text not null,
    primary key(question_id, status_user)
);

create table answers (
    question_id integer not null references questions (id),
    index integer not null,
    vote_count integer not null,
    text text not null,
    html text not null,
    accepted boolean not null,
    date date not null,
    community_question boolean not null,
    user text not null,
    user_reputation integer not null,
    primary key (question_id, index)
);

create table question_links (
    question_id integer not null references questions (id),
    other_question_id integer not null,
    primary key (question_id, other_question_id)
);

create table question_related (
    question_id integer not null references questions (id),
    other_question_id integer not null,
    primary key (question_id, other_question_id)
);

create table question_comments (
    question_id integer not null references questions (id),
    comment_id integer not null,
    text text not null,
    html text not null,
    date date not null,
    user text not null,
    user_reputation integer,
    primary key (question_id, comment_id)
)