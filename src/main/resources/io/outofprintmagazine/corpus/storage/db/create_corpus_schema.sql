create table if not exists corpus (
corpus_id varchar(50)
, name varchar(500)
, description text
, primary key(corpus_id)
);

create table if not exists corpus_documents (
corpus_id varchar(50)
, document_id varchar(50)
, primary key (corpus_id, document_id)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);

create table if not exists corpus_scores_raw (
corpus_id varchar(50)
, score varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);

create table if not exists corpus_subscores_raw (
corpus_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score, subscore)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);

create table if not exists corpus_scores_normalized (
corpus_id varchar(50)
, score varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);

create table if not exists corpus_subscores_normalized (
corpus_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score, subscore)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);


create table if not exists corpus_scores_count (
corpus_id varchar(50)
, score varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);

create table if not exists corpus_subscores_count (
corpus_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score, subscore)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);


create table if not exists corpus_scores_rank (
corpus_id varchar(50)
, score varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);

create table if not exists corpus_subscores_rank (
corpus_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score, subscore)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);


create table if not exists corpus_scores_percentage (
corpus_id varchar(50)
, score varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);

create table if not exists corpus_subscores_percentage (
corpus_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score, subscore)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);


create table if not exists corpus_scores_percentile (
corpus_id varchar(50)
, score varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);

create table if not exists corpus_subscores_percentile (
corpus_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_value numeric
, score_count numeric
, score_min numeric
, score_max numeric
, score_sparse_mean numeric
, score_sparse_median numeric
, score_sparse_stddev numeric
, score_dense_mean numeric
, score_dense_median numeric
, score_dense_stddev numeric
, primary key (corpus_id, score, subscore)
, foreign key (corpus_id) references corpus(corpus_id) ON DELETE CASCADE
);

create table if not exists document_tfdif_scores (
document_id varchar(50)
, score varchar(50)
, score_tfidf numeric
, primary key (document_id, score)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_tfdif_subscores (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_tfidf numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_sparse_z_subscores_raw (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_dense_z_subscores_raw (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_sparse_z_subscores_normalized (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_dense_z_subscores_normalized (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_sparse_z_subscores_count (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_dense_z_subscores_count (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_sparse_z_subscores_rank (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_dense_z_subscores_rank (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_sparse_z_subscores_percentage (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_dense_z_subscores_percentage (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_sparse_z_subscores_percentile (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_dense_z_subscores_percentile (
document_id varchar(50)
, score varchar(50)
, subscore varchar(50)
, score_z numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);