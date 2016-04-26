DELIMITER $$

USE `bi`$$

DROP PROCEDURE IF EXISTS `BI_DRPT_NACT_FANLI`$$

CREATE DEFINER=`tf8rpt`@`%` PROCEDURE `BI_DRPT_NACT_FANLI`(t DATE)
BEGIN
    DECLARE done  INT DEFAULT 0;
    DECLARE v_date_control VARCHAR(10);
    DECLARE p DATE;
	DECLARE v_act_id	VARCHAR(50);
	DECLARE v_act_code	VARCHAR(200);
	DECLARE v_fanli_time	DATE;
	DECLARE v_trade_create_time	DATETIME;
	DECLARE v_mms_fanli_id	BIGINT;
	DECLARE v_client_type	VARCHAR(50);
	DECLARE v_page	VARCHAR(300);
	DECLARE v_user_id	BIGINT;
	DECLARE v_item_id	BIGINT(20);
	DECLARE v_item_title	VARCHAR(200);
	DECLARE v_item_num	BIGINT;
	DECLARE v_real_pay_fee	DOUBLE;
	DECLARE v_real_name	BIGINT;
	DECLARE v_seller_nick	VARCHAR(200);
	DECLARE v_match_type INT;
	DECLARE v_tf8_level	INT UNSIGNED;
	DECLARE v_user_category	VARCHAR(50);
	DECLARE v_bm_category	INT UNSIGNED;
	DECLARE v_sub_category_id	BIGINT;
	DECLARE v_tfb_rate	BIGINT;
	DECLARE v_item_category	VARCHAR(50);
	DECLARE v_category_name	VARCHAR(200);
	DECLARE v_cnt INT;
	DECLARE v_start_time DATETIME;
	DECLARE v_end_time DATETIME;
	DECLARE v_category_id INT;
	DECLARE v_tfb_rate_type INT;
	
    DECLARE cur CURSOR FOR SELECT real_name,trade_create_time,mms_fanli_id,item_id,user_id,fanli_time,page,match_type FROM  bi.bi_drpt_nact_fanli WHERE fanli_time=p ;  
    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = 1;
    SET p=DATE_SUB(t,INTERVAL 1 DAY);
    SET v_date_control=YEAR(p)*100+MONTH(p);
    #插入活动返利数据
    INSERT INTO bi.bi_drpt_nact_fanli(fanli_time,trade_create_time,mms_fanli_id,client_type,page,user_id,item_id,item_title,item_num,real_pay_fee,seller_nick,match_type,real_name) 
    SELECT fanli_time,trade_create_time,fanli_id,client_type,page,user_id,item_id,item_title,item_num,real_pay_fee,seller_nick,match_type,real_name FROM mms.mms_fanli WHERE fanli_time=p AND page LIKE '%nact%';
    #建立视图取用户打标数据
	DROP VIEW IF EXISTS tmp_dkl_20160315;
	SET @sql_text_11='CREATE VIEW tmp_dkl_20160315 AS select * from bi.bi_mrpt_user_mark_';
	SET @sql_text_12=CONCAT(@sql_text_11,v_date_control);
	PREPARE stmt FROM @sql_text_12;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
  OPEN cur;
        REPEAT 
        FETCH cur INTO v_real_name,v_trade_create_time,v_mms_fanli_id,v_item_id,v_user_id,v_fanli_time,v_page,v_match_type;
IF NOT done THEN
#获取活动act_code,act_id,start_time,end_time;
	SELECT LEFT(SUBSTR(v_page,INSTR(v_page,'-')+1),
	INSTR((SUBSTR(v_page,INSTR(v_page,'-')+1)),'_')-1) INTO v_act_code;
	SELECT COUNT(*) INTO v_cnt FROM mms.mms_activity WHERE act_code=v_act_code;
		IF v_cnt>0 THEN
		SELECT id,start_time,end_time INTO v_act_id,v_start_time,v_end_time FROM mms.mms_activity WHERE act_code=v_act_code;
		ELSE 
		SET v_act_id=NULL;
		END IF;
#获取tf8_level mms.mms_users_tfb_level
	SELECT COUNT(*) INTO v_cnt FROM mms.mms_users_tfb_level  WHERE user_id=v_user_id;
		IF v_cnt>0 THEN 
		SELECT tf8_level INTO v_tf8_level FROM mms.mms_users_tfb_level  WHERE user_id=v_user_id;
		ELSE SET v_tf8_level=NULL;
		END IF;
		
#获取user_category
SELECT COUNT(*) INTO v_cnt FROM tmp_dkl_20160315 WHERE user_id=v_user_id;
IF v_cnt>0 THEN 
SELECT user_mark INTO v_user_category  FROM tmp_dkl_20160315 WHERE user_id=v_user_id;
ELSE
		 SET v_user_category='new';
		
END IF;
#获取item_category
IF v_match_type= 30 THEN 
SET v_item_category='聚划算';
ELSE
	SELECT COUNT(*) INTO v_cnt FROM mms.mms_items WHERE id=v_real_name;
	IF v_cnt>0 THEN 
	        SELECT category_id,tfb_rate,tfb_rate_type INTO v_category_id,v_tfb_rate,v_tfb_rate_type FROM mms.mms_items WHERE id=v_real_name;
		IF v_category_id=20 THEN 
		  SET v_item_category='超高返品牌团';
		ELSEIF v_tfb_rate>=7000 OR v_tfb_rate_type=1 THEN
		   SET v_item_category='超高返单品';
		ELSE
		SET v_item_category='普返';
		END IF;
	ELSE SET v_item_category=NULL;
	END IF;
END IF;	
#取sub_category_id
SELECT COUNT(*) INTO v_cnt FROM mms.mms_items WHERE id=v_real_name AND category_id=20;
IF v_cnt>0 THEN 
SELECT sub_category_id INTO v_sub_category_id FROM mms.mms_items WHERE id=v_real_name AND category_id=20;
ELSE SET v_sub_category_id=NULL;
END IF;
#取tfb_rate
SELECT COUNT(*) INTO v_cnt FROM mms.mms_items WHERE id=v_real_name AND tfb_rate IS NOT NULL;
IF v_cnt>0 THEN 
SELECT tfb_rate INTO v_tfb_rate FROM mms.mms_items WHERE id=v_real_name AND tfb_rate IS NOT NULL;
ELSE SET v_tfb_rate=NULL;
END IF;
#bm_category,,category_name
SELECT COUNT(*) INTO v_cnt FROM mms.mms_items WHERE id=v_real_name AND bm_category>0;
IF v_cnt>0 THEN 
SELECT a.bm_category,b.category_name INTO v_bm_category,v_category_name FROM mms.mms_items a,mms.mms_bm_category b
WHERE a.bm_category=b.category_code AND a.id=v_real_name;
ELSE SET v_bm_category=NULL;
SET v_category_name=NULL;
END IF;
#更新语句
UPDATE bi_drpt_nact_fanli SET act_id=v_act_id,act_code=v_act_code,tf8_level=v_tf8_level,user_category=v_user_category,item_category=v_item_category,sub_category_id=v_sub_category_id,tfb_rate=v_tfb_rate,bm_category=v_bm_category,category_name=v_category_name WHERE mms_fanli_id=v_mms_fanli_id;
END IF;
UNTIL done END REPEAT;
CLOSE cur;
UPDATE bi_drpt_nact_fanli   SET user_category='lost' WHERE user_category='new' AND tf8_level>=1 AND fanli_time=p;
    END$$

DELIMITER ;