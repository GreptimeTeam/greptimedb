-- A 49-level value stays fully structured because it is below the limit.
create table json2_depth_49 (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json2_depth_49 values (
    1,
    '{"level_01":{"level_02":{"level_03":{"level_04":{"level_05":{"level_06":{"level_07":{"level_08":{"level_09":{"level_10":{"level_11":{"level_12":{"level_13":{"level_14":{"level_15":{"level_16":{"level_17":{"level_18":{"level_19":{"level_20":{"level_21":{"level_22":{"level_23":{"level_24":{"level_25":{"level_26":{"level_27":{"level_28":{"level_29":{"level_30":{"level_31":{"level_32":{"level_33":{"level_34":{"level_35":{"level_36":{"level_37":{"level_38":{"level_39":{"level_40":{"level_41":{"level_42":{"level_43":{"level_44":{"level_45":{"level_46":{"level_47":{"level_48":{"level_49":1}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}'
);

select j.level_01 from json2_depth_49;

admin flush_table('json2_depth_49');

select j.level_01 from json2_depth_49;

drop table json2_depth_49;

-- A 50-level value with a scalar at the boundary stays fully structured.
create table json2_depth_50 (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json2_depth_50 values (
    1,
    '{"level_01":{"level_02":{"level_03":{"level_04":{"level_05":{"level_06":{"level_07":{"level_08":{"level_09":{"level_10":{"level_11":{"level_12":{"level_13":{"level_14":{"level_15":{"level_16":{"level_17":{"level_18":{"level_19":{"level_20":{"level_21":{"level_22":{"level_23":{"level_24":{"level_25":{"level_26":{"level_27":{"level_28":{"level_29":{"level_30":{"level_31":{"level_32":{"level_33":{"level_34":{"level_35":{"level_36":{"level_37":{"level_38":{"level_39":{"level_40":{"level_41":{"level_42":{"level_43":{"level_44":{"level_45":{"level_46":{"level_47":{"level_48":{"level_49":{"level_50":1}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}'
);

select j.level_01 from json2_depth_50;

admin flush_table('json2_depth_50');

select j.level_01 from json2_depth_50;

drop table json2_depth_50;

-- At 51 levels, the object at level 50 becomes a JSONB Variant.
create table json2_depth_51 (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json2_depth_51 values (
    1,
    '{"level_01":{"level_02":{"level_03":{"level_04":{"level_05":{"level_06":{"level_07":{"level_08":{"level_09":{"level_10":{"level_11":{"level_12":{"level_13":{"level_14":{"level_15":{"level_16":{"level_17":{"level_18":{"level_19":{"level_20":{"level_21":{"level_22":{"level_23":{"level_24":{"level_25":{"level_26":{"level_27":{"level_28":{"level_29":{"level_30":{"level_31":{"level_32":{"level_33":{"level_34":{"level_35":{"level_36":{"level_37":{"level_38":{"level_39":{"level_40":{"level_41":{"level_42":{"level_43":{"level_44":{"level_45":{"level_46":{"level_47":{"level_48":{"level_49":{"level_50":{"level_51":1}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}'
);

select j.level_01 from json2_depth_51;

-- The capped JSONB object should still be readable as JSON, and its leaf should remain queryable.
select json_get(j, 'level_01.level_02.level_03.level_04.level_05.level_06.level_07.level_08.level_09.level_10.level_11.level_12.level_13.level_14.level_15.level_16.level_17.level_18.level_19.level_20.level_21.level_22.level_23.level_24.level_25.level_26.level_27.level_28.level_29.level_30.level_31.level_32.level_33.level_34.level_35.level_36.level_37.level_38.level_39.level_40.level_41.level_42.level_43.level_44.level_45.level_46.level_47.level_48.level_49.level_50') from json2_depth_51;

select json_get(j, 'level_01.level_02.level_03.level_04.level_05.level_06.level_07.level_08.level_09.level_10.level_11.level_12.level_13.level_14.level_15.level_16.level_17.level_18.level_19.level_20.level_21.level_22.level_23.level_24.level_25.level_26.level_27.level_28.level_29.level_30.level_31.level_32.level_33.level_34.level_35.level_36.level_37.level_38.level_39.level_40.level_41.level_42.level_43.level_44.level_45.level_46.level_47.level_48.level_49.level_50.level_51')::int64 from json2_depth_51;

admin flush_table('json2_depth_51');

select j.level_01 from json2_depth_51;

select json_get(j, 'level_01.level_02.level_03.level_04.level_05.level_06.level_07.level_08.level_09.level_10.level_11.level_12.level_13.level_14.level_15.level_16.level_17.level_18.level_19.level_20.level_21.level_22.level_23.level_24.level_25.level_26.level_27.level_28.level_29.level_30.level_31.level_32.level_33.level_34.level_35.level_36.level_37.level_38.level_39.level_40.level_41.level_42.level_43.level_44.level_45.level_46.level_47.level_48.level_49.level_50') from json2_depth_51;

select json_get(j, 'level_01.level_02.level_03.level_04.level_05.level_06.level_07.level_08.level_09.level_10.level_11.level_12.level_13.level_14.level_15.level_16.level_17.level_18.level_19.level_20.level_21.level_22.level_23.level_24.level_25.level_26.level_27.level_28.level_29.level_30.level_31.level_32.level_33.level_34.level_35.level_36.level_37.level_38.level_39.level_40.level_41.level_42.level_43.level_44.level_45.level_46.level_47.level_48.level_49.level_50.level_51')::int64 from json2_depth_51;

drop table json2_depth_51;

-- A much deeper value is also capped at level 50 instead of growing the Arrow schema.
create table json2_depth_70 (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json2_depth_70 values (
    1,
    '{"level_01":{"level_02":{"level_03":{"level_04":{"level_05":{"level_06":{"level_07":{"level_08":{"level_09":{"level_10":{"level_11":{"level_12":{"level_13":{"level_14":{"level_15":{"level_16":{"level_17":{"level_18":{"level_19":{"level_20":{"level_21":{"level_22":{"level_23":{"level_24":{"level_25":{"level_26":{"level_27":{"level_28":{"level_29":{"level_30":{"level_31":{"level_32":{"level_33":{"level_34":{"level_35":{"level_36":{"level_37":{"level_38":{"level_39":{"level_40":{"level_41":{"level_42":{"level_43":{"level_44":{"level_45":{"level_46":{"level_47":{"level_48":{"level_49":{"level_50":{"level_51":{"level_52":{"level_53":{"level_54":{"level_55":{"level_56":{"level_57":{"level_58":{"level_59":{"level_60":{"level_61":{"level_62":{"level_63":{"level_64":{"level_65":{"level_66":{"level_67":{"level_68":{"level_69":{"level_70":1}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}'
);

select j.level_01 from json2_depth_70;

-- Paths beyond the cap should traverse the JSONB Variant to the deepest leaf.
select json_get(j, 'level_01.level_02.level_03.level_04.level_05.level_06.level_07.level_08.level_09.level_10.level_11.level_12.level_13.level_14.level_15.level_16.level_17.level_18.level_19.level_20.level_21.level_22.level_23.level_24.level_25.level_26.level_27.level_28.level_29.level_30.level_31.level_32.level_33.level_34.level_35.level_36.level_37.level_38.level_39.level_40.level_41.level_42.level_43.level_44.level_45.level_46.level_47.level_48.level_49.level_50.level_51.level_52.level_53.level_54.level_55.level_56.level_57.level_58.level_59.level_60.level_61.level_62.level_63.level_64.level_65.level_66.level_67.level_68.level_69.level_70')::int64 from json2_depth_70;

admin flush_table('json2_depth_70');

select j.level_01 from json2_depth_70;

select json_get(j, 'level_01.level_02.level_03.level_04.level_05.level_06.level_07.level_08.level_09.level_10.level_11.level_12.level_13.level_14.level_15.level_16.level_17.level_18.level_19.level_20.level_21.level_22.level_23.level_24.level_25.level_26.level_27.level_28.level_29.level_30.level_31.level_32.level_33.level_34.level_35.level_36.level_37.level_38.level_39.level_40.level_41.level_42.level_43.level_44.level_45.level_46.level_47.level_48.level_49.level_50.level_51.level_52.level_53.level_54.level_55.level_56.level_57.level_58.level_59.level_60.level_61.level_62.level_63.level_64.level_65.level_66.level_67.level_68.level_69.level_70')::int64 from json2_depth_70;

drop table json2_depth_70;
