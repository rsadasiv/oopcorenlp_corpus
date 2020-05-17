create table if not exists staging_batches(
corpus_batch_id varchar(50)
, name varchar(500)
, description text
, json_data jsonb
, primary key(corpus_batch_id)
);

create table if not exists staging_batch_items(
corpus_batch_id varchar(50) 
, corpus_batch_step_id varchar(50)
, corpus_batch_step_sequence_id int
, corpus_batch_step_class varchar(500)
, properties jsonb 
, primary key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class)
, foreign key (corpus_batch_id) references staging_batches(corpus_batch_id) ON DELETE CASCADE
);

create table if not exists staging_batch_item_input(
corpus_batch_id varchar(50)
, corpus_batch_step_id varchar(50)
, corpus_batch_step_sequence_id int
, corpus_batch_step_class varchar(500)
, staging_batch_item_input_id int
, property_name varchar(500)
, property_value text
, primary key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class, staging_batch_item_input_id, property_name) 
, foreign key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class) 
references staging_batch_items(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class) ON DELETE CASCADE
);

create table if not exists staging_batch_item_output(
corpus_batch_id varchar(50) 
, corpus_batch_step_id varchar(50)
, corpus_batch_step_sequence_id int
, corpus_batch_step_class varchar(500)
, staging_batch_item_output_id int
, property_name varchar(500)
, property_value text
, primary key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class, staging_batch_item_output_id, property_name) 
, foreign key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class) 
references staging_batch_items(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class) ON DELETE CASCADE
);

create table if not exists staging_batch_documents(
corpus_batch_id varchar(50)
, document_id varchar(50)
, status varchar(50)
, primary key(corpus_batch_id, document_id)
, foreign key (corpus_batch_id) references staging_batches(corpus_batch_id) ON DELETE CASCADE
);

create table if not exists staging_batch_document_text(
corpus_batch_id varchar(50)
, document_id varchar(50)
, data text
, data_tokens TSVECTOR
, primary key(corpus_batch_id, document_id)
, foreign key (corpus_batch_id, document_id) references staging_batch_documents(corpus_batch_id, document_id) ON DELETE CASCADE
);

create table if not exists staging_batch_document_properties(
corpus_batch_id varchar(50)
, document_id varchar(50)
, property_name varchar(50)
, property_value varchar(50)
, primary key(corpus_batch_id, document_id, property_name)
, foreign key (corpus_batch_id, document_id) references staging_batch_documents(corpus_batch_id, document_id) ON DELETE CASCADE
);