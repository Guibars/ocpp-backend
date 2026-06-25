-- Fotus Charge OCPP Backend - Supabase schema
-- Run this file in Supabase SQL Editor before enabling SUPABASE_URL on Railway.

create extension if not exists "pgcrypto";

create table if not exists public.chargers (
  id uuid primary key default gen_random_uuid(),
  charge_point_id text not null unique,
  fabricante text default 'Desconhecido',
  modelo text default 'Desconhecido',
  status text default 'Offline',
  ultimo_heartbeat timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.connectors (
  id uuid primary key default gen_random_uuid(),
  charger_id uuid references public.chargers(id) on delete cascade,
  charge_point_id text not null,
  connector_number integer not null default 1,
  status text default 'Unknown',
  error_code text default 'NoError',
  info text,
  vendor_error_code text,
  timestamp timestamptz,
  type text default 'CCS2',
  power_kw numeric default 50,
  price_per_kwh numeric,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (charger_id, connector_number)
);

create table if not exists public.charger_locations (
  charge_point_id text primary key,
  name text,
  address text,
  lat double precision not null,
  lng double precision not null,
  network text default 'Fotus',
  connector_type text default 'CCS2',
  power_kw numeric default 50,
  price_per_kwh numeric default 2.1,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.ocpp_events (
  id uuid primary key default gen_random_uuid(),
  charge_point_id text not null,
  direction text,
  action text not null,
  payload jsonb default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.charging_sessions (
  id uuid primary key default gen_random_uuid(),
  charger_id uuid references public.chargers(id) on delete set null,
  charge_point_id text not null,
  connector_id integer default 1,
  transaction_id bigint,
  id_tag text,
  started_at timestamptz,
  ended_at timestamptz,
  meter_start_kwh numeric default 0,
  meter_stop_kwh numeric,
  energy_kwh numeric default 0,
  current_power_kw numeric default 0,
  price_per_kwh numeric default 0,
  current_cost numeric default 0,
  tariff_name text,
  status text default 'Charging',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (transaction_id)
);

create table if not exists public.meter_values (
  id uuid primary key default gen_random_uuid(),
  charge_point_id text not null,
  transaction_id bigint,
  connector_id integer,
  timestamp timestamptz,
  energy_kwh numeric,
  power_kw numeric,
  voltage numeric,
  current numeric,
  raw_payload jsonb default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.command_results (
  id uuid primary key default gen_random_uuid(),
  message_id text unique,
  charge_point_id text not null,
  action text,
  status text,
  request_payload jsonb default '{}'::jsonb,
  response_payload jsonb default '{}'::jsonb,
  error_code text,
  error_description text,
  error_details jsonb default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.tariffs (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  start_hour integer not null,
  end_hour integer not null,
  price_per_kwh numeric not null,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_connectors_charge_point_id on public.connectors(charge_point_id);
create index if not exists idx_ocpp_events_charge_point_created on public.ocpp_events(charge_point_id, created_at desc);
create index if not exists idx_sessions_charge_point_status on public.charging_sessions(charge_point_id, status);
create index if not exists idx_meter_values_transaction on public.meter_values(transaction_id, created_at desc);
create index if not exists idx_command_results_message_id on public.command_results(message_id);

alter table public.chargers enable row level security;
alter table public.connectors enable row level security;
alter table public.charger_locations enable row level security;
alter table public.ocpp_events enable row level security;
alter table public.charging_sessions enable row level security;
alter table public.meter_values enable row level security;
alter table public.command_results enable row level security;
alter table public.tariffs enable row level security;

insert into public.tariffs (name, start_hour, end_hour, price_per_kwh, active)
values
  ('Madrugada', 0, 6, 1.5, true),
  ('Horario Comercial', 6, 18, 2.2, true),
  ('Horario de Ponta', 18, 22, 2.8, true),
  ('Noite', 22, 24, 1.9, true)
on conflict do nothing;
