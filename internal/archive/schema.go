package archive

const archiveSchema = `
create table if not exists ledger_entries (
  id integer primary key,
  exchange text not null,
  account_id text not null,
  account_label text not null,
  entry_id text not null,
  symbol text not null,
  category text not null,
  entry_type text not null,
  asset text not null,
  side text not null,
  amount text not null,
  fee text not null,
  funding text not null,
  cash_flow text not null,
  balance text not null,
  order_id text not null,
  trade_id text not null,
  info text not null,
  occurred_at integer not null,
  observed_at integer not null,
  raw_json text not null,
  unique(exchange, account_id, entry_id)
);

create index if not exists idx_ledger_entries_order
  on ledger_entries(exchange, occurred_at desc, account_id, entry_id desc);
create index if not exists idx_ledger_entries_account_order
  on ledger_entries(exchange, account_id, occurred_at desc, entry_id desc);
create index if not exists idx_ledger_entries_symbol_order
  on ledger_entries(exchange, symbol, occurred_at desc, entry_id desc);
create index if not exists idx_ledger_entries_type_order
  on ledger_entries(exchange, entry_type, occurred_at desc, entry_id desc);

create virtual table if not exists ledger_entries_fts using fts5(
  symbol,
  category,
  entry_type,
  asset,
  side,
  order_id,
  trade_id,
  info,
  content='ledger_entries',
  content_rowid='id'
);

create trigger if not exists ledger_entries_ai after insert on ledger_entries begin
  insert into ledger_entries_fts(
    rowid, symbol, category, entry_type, asset, side, order_id, trade_id, info
  ) values (
    new.id, new.symbol, new.category, new.entry_type, new.asset, new.side,
    new.order_id, new.trade_id, new.info
  );
end;

create trigger if not exists ledger_entries_ad after delete on ledger_entries begin
  insert into ledger_entries_fts(
    ledger_entries_fts, rowid, symbol, category, entry_type, asset, side,
    order_id, trade_id, info
  ) values (
    'delete', old.id, old.symbol, old.category, old.entry_type, old.asset,
    old.side, old.order_id, old.trade_id, old.info
  );
end;

create trigger if not exists ledger_entries_au after update on ledger_entries begin
  insert into ledger_entries_fts(
    ledger_entries_fts, rowid, symbol, category, entry_type, asset, side,
    order_id, trade_id, info
  ) values (
    'delete', old.id, old.symbol, old.category, old.entry_type, old.asset,
    old.side, old.order_id, old.trade_id, old.info
  );
  insert into ledger_entries_fts(
    rowid, symbol, category, entry_type, asset, side, order_id, trade_id, info
  ) values (
    new.id, new.symbol, new.category, new.entry_type, new.asset, new.side,
    new.order_id, new.trade_id, new.info
  );
end;
`
