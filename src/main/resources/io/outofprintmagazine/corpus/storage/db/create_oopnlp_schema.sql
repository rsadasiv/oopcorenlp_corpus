create table if not exists scores (
score varchar(50)
, description varchar(1000)
, primary key (score)
);

create table if not exists documents (
document_id varchar(50)
, primary key (document_id)
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

create table if not exists document_aggregate_lists (
document_id varchar(50)
, score varchar(50) references scores(score)
, subscore varchar(500)
, subscore_raw numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_aggregate_subscores (
document_id varchar(50)
, score varchar(50) references scores(score)
, subscore varchar(500)
, subscore_raw numeric
, subscore_normalized numeric
, subscore_count numeric
, subscore_rank numeric
, subscore_percentage numeric
, subscore_percentile numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_aggregate_scores_z (
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


create table if not exists document_aggregate_subscores_z (
document_id varchar(50)
, score varchar(50) references scores(score)
, subscore varchar(500)
, subscore_raw numeric
, subscore_normalized numeric
, subscore_count numeric
, subscore_rank numeric
, subscore_percentage numeric
, subscore_percentile numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);


create table if not exists document_aggregate_subscores_tfidf (
document_id varchar(50)
, score varchar(50) references scores(score)
, subscore varchar(500)
, subscore_tfidf numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_aggregate_subscores_myers_briggs (
document_id varchar(50)
, score varchar(50) references scores(score)
, subscore varchar(500)
, subscore_myers_briggs numeric
, primary key (document_id, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);

create table if not exists document_actor_aggregate_subscores_myers_briggs (
document_id varchar(50)
, actor varchar(500)
, score varchar(50) references scores(score)
, subscore varchar(500)
, subscore_myers_briggs numeric
, primary key (document_id, actor, score, subscore)
, foreign key (document_id) references documents(document_id) ON DELETE CASCADE
);
