create extension if not exists citext;

create schema if not exists test123;

create table if not exists test123.users (
    id bigserial primary key,
    username citext unique not null,
    created_at timestamptz not null default now()
);



alter table test123.users 
    add column if not exists irgendwas text,
    add column if not exists irgendwas2 text; --oebbis