create table if not exists staging_batches(corpus_batch_id varchar(50), primary key(corpus_batch_id));

create table if not exists staging_batch_items(
corpus_batch_id varchar(50) 
, corpus_batch_step_id varchar(50)
, corpus_batch_step_sequence_id int
, corpus_batch_step_class varchar(50)
, properties json 
, primary key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class)
, foreign key (corpus_batch_id) references staging_batches(corpus_batch_id) ON DELETE CASCADE
);

create table if not exists staging_batch_item_input(
corpus_batch_id varchar(50)
, corpus_batch_step_id varchar(50)
, corpus_batch_step_sequence_id int
, corpus_batch_step_class varchar(50)
, data jsonb 
, primary key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class, data) 
, foreign key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class) 
references staging_batch_items(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class)
);

create table if not exists staging_batch_item_output(
corpus_batch_id varchar(50) 
, corpus_batch_step_id varchar(50)
, corpus_batch_step_sequence_id int
, corpus_batch_step_class varchar(50)
, data jsonb 
, primary key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class, data) 
, foreign key(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class) 
references staging_batch_items(corpus_batch_id, corpus_batch_step_id, corpus_batch_step_sequence_id, corpus_batch_step_class) ON DELETE CASCADE
);

create table if not exists documents (
document_id varchar(50)
, primary key (document_id)
);

create table if not exists document_properties (
document_id varchar(50) 
, property_name varchar(50)
, property_value varchar(50)
, primary key (document_id, property_name)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists scores (
score varchar(50)
, description varchar(1000)
, primary key (score)
);

create table if not exists core_nlp(
corpus_batch_id varchar(50)
, document_id varchar(50)
, data jsonb 
, primary key (corpus_batch_id, document_id)
, foreign key (corpus_batch_id) references staging_batches(corpus_batch_id) ON DELETE CASCADE
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists oop_nlp(
corpus_batch_id varchar(50)
, document_id varchar(50)
, data jsonb 
, primary key(corpus_batch_id, document_id)
, foreign key (corpus_batch_id) references staging_batches(corpus_batch_id) ON DELETE CASCADE
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists plain_text(
corpus_batch_id varchar(50)
, document_id varchar(50)
, data text
, data_tokens TSVECTOR
, primary key(corpus_batch_id, document_id)
, foreign key (corpus_batch_id) references staging_batches(corpus_batch_id) ON DELETE CASCADE
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists pipeline_info(
corpus_batch_id varchar(50)
, document_id varchar(50)
, data jsonb
, primary key(corpus_batch_id, document_id)
, foreign key (corpus_batch_id) references staging_batches(corpus_batch_id) ON DELETE CASCADE
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_scores (
document_id varchar(50)
, score varchar(50) references scores(score)
, score_raw numeric
, score_normalized numeric
, primary key (document_id, score)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_subscores (
document_id varchar(50)
, score varchar(50) references scores(score)
, subscore varchar(50)
, subscore_raw numeric
, subscore_normalized numeric
, primary key (document_id, score)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_sentences (
document_id varchar(50)
, sentence_id numeric
, sentence_text varchar(1000)
, primary key (document_id, sentence_id)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_sentence_tokens (
document_id varchar(50)
, sentence_id numeric
, token_id numeric
, word varchar(50)
, originalText varchar(50)
, lemma varchar(50)
, characterOffsetBegin numeric
, characterOffsetEnd numeric
, pos varchar(5)
, ner varchar(50)
, before varchar(1)
, after varchar(1)
, primary key (document_id, sentence_id, token_id)
, foreign key(document_id, sentence_id) 
references document_sentences(document_id, sentence_id) ON DELETE CASCADE
);

create table if not exists document_sentence_scores (
document_id varchar(50) 
, sentence_id numeric 
, score varchar(50) references scores(score)
, score_raw numeric
, score_normalized numeric
, primary key (document_id, sentence_id, score)
, foreign key(document_id, sentence_id) 
references document_sentences(document_id, sentence_id)
);

create table if not exists document_sentence_subscores (
document_id varchar(50)
, sentence_id numeric
, score varchar(50) references scores(score)
, subscore varchar(50)
, subscore_raw numeric
, subscore_normalized numeric
, primary key (document_id, sentence_id, score, subscore)
, foreign key(document_id, sentence_id) 
references document_sentences(document_id, sentence_id)
);

create table if not exists document_sentence_token_scores (
document_id varchar(50)
, sentence_id numeric
, token_id numeric
, score varchar(50) references scores(score)
, score_raw numeric
, score_normalized numeric
, primary key (document_id, sentence_id, token_id, score)
, foreign key (document_id, sentence_id, token_id) 
references document_sentence_tokens(document_id, sentence_id, token_id) ON DELETE CASCADE
);

create table if not exists document_sentence_token_subscores (
document_id varchar(50)
, sentence_id numeric
, token_id numeric
, score varchar(50) references scores(score)
, subscore varchar(50)
, subscore_raw numeric
, subscore_normalized numeric
, primary key (document_id, sentence_id, token_id, score, subscore)
, foreign key (document_id, sentence_id, token_id) 
references document_sentence_tokens(document_id, sentence_id, token_id) ON DELETE CASCADE
);

create table if not exists document_sentence_token_syllable_scores (
document_id varchar(50)
, sentence_id numeric
, token_id numeric
, syllable_id numeric 
, score varchar(50) references scores(score)
, score_raw numeric
, score_normalized numeric
, primary key (document_id, sentence_id, token_id, syllable_id, score)
, foreign key (document_id, sentence_id, token_id) 
references document_sentence_tokens(document_id, sentence_id, token_id) ON DELETE CASCADE
);

create table if not exists document_sentence_token_syllable_subscores (
document_id varchar(50)
, sentence_id numeric
, token_id numeric
, syllable_id numeric
, score varchar(50) references scores(score)
, subscore varchar(50)
, subscore_raw numeric
, subscore_normalized numeric
, primary key (document_id, sentence_id, token_id, syllable_id, score, subscore)
, foreign key (document_id, sentence_id, token_id) 
references document_sentence_tokens(document_id, sentence_id, token_id) ON DELETE CASCADE
);

create table if not exists document_aggregate_scores (
document_id varchar(50)
, score varchar(50) references scores(score)
, score_raw numeric
, score_normalized numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_mean numeric
, score_median numeric
, score_stddev numeric
, primary key (document_id, score)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_aggregate_subscores (
document_id varchar(50)
, score varchar(50) references scores(score)
, subscore varchar(50)
, subscore_raw numeric
, subscore_normalized numeric
, subscore_count numeric
, subscore_rank numeric
, subscore_percentage numeric
, subscore_percentile numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists corpus_aggregate_scores (
score varchar(50) references scores(score)
, score_raw numeric
, score_normalized numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_mean numeric
, score_median numeric
, score_stddev numeric
, primary key (score)
);

create table if not exists corpus_aggregate_subscores (
score varchar(50) references scores(score)
, subscore varchar(50)
, subscore_raw numeric
, subscore_normalized numeric
, subscore_count numeric
, subscore_rank numeric
, subscore_percentage numeric
, subscore_percentile numeric
, primary key (score, subscore)
);


