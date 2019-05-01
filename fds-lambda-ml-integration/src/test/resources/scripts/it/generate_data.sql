/*
 * This script generates data for uploading to API url.
 * Generates data for bidding, impressions and clicks.
 */

create table app (
	id text primary key
);

insert into app values 
	select uuid_generate_v4() from generate_series(1, 5);

create  table campaign_item (
	id text primary key
);

insert into campaign_item values 
	select uuid_generate_v4() from generate_series(1, 5);


/*
create table bid (
	txid text primary key,
	appuid text not null references app(id),
	campaign_item_id text,
	
);

*/

