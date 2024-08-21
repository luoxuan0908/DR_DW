
select * from odps_table_test;

drop table if EXISTS  whde.odps_table_test;
CREATE TABLE IF NOT EXISTS whde.odps_table_test(
                                                   date STRING,
                                                   attributedsalessamesku1d STRING,
                                                   roasclicks14d STRING,
                                                   unitssoldclicks1d STRING,
                                                   matchtype STRING,
                                                   attributedsalessamesku14d STRING,
                                                   sales7d STRING,
                                                   attributedsalessamesku30d STRING,
                                                   kindleeditionnormalizedpagesroyalties14d STRING,
                                                   searchterm STRING,
                                                   unitssoldsamesku1d STRING,
                                                   campaignstatus STRING,
                                                   keyword STRING,
                                                   salesothersku7d STRING,
                                                   purchasessamesku7d STRING,
                                                   campaignbudgetamount STRING,
                                                   purchases7d STRING,
                                                   unitssoldsamesku30d STRING,
                                                   costperclick STRING,
                                                   unitssoldclicks14d STRING,
                                                   adGroupName STRING,
                                                   campaignid STRING,
                                                   clickthroughrate STRING,
                                                   kindleeditionnormalizedpagesread14d STRING,
                                                   acosclicks14d STRING,
                                                   unitssoldclicks30d STRING,
                                                   portfolioid STRING,
                                                   campaignbudgetcurrencycode STRING,
                                                   roasclicks7d STRING,
                                                   unitssoldsamesku14d STRING,
                                                   unitssoldclicks7d STRING,
                                                   keywordid STRING,
                                                   attributedsalessamesku7d STRING,
                                                   sales1d STRING,
                                                   adgroupid STRING,
                                                   keywordbid STRING,
                                                   targeting STRING,
                                                   purchasessamesku14d STRING,
                                                   unitssoldothersku7d STRING,
                                                   purchasessamesku1d STRING,
                                                   campaignbudgettype STRING,
                                                   adkeywordstatus STRING,
                                                   keywordtype STRING,
                                                   purchases1d STRING,
                                                   unitssoldsamesku7d STRING,
                                                   cost STRING,
                                                   sales14d STRING,
                                                   acosclicks7d STRING,
                                                   sales30d STRING,
                                                   impressions STRING,
                                                   purchasessamesku30d STRING,
                                                   purchases14d STRING,
                                                   purchases30d STRING,
                                                   clicks STRING,
                                                   campaignName STRING )
    STORED AS ALIORC
    LIFECYCLE 7;


drop table if EXISTS  whde.odps_table_test;
CREATE TABLE IF NOT EXISTS whde.odps_table_test(
                                                   date  STRING
    ,attributedSalesSameSku1d  STRING
    ,roasClicks14d  STRING
    ,unitsSoldClicks1d  STRING
    ,attributedSalesSameSku14d  STRING
    ,sales7d  STRING
    ,attributedSalesSameSku30d  STRING
    ,kindleEditionNormalizedPagesRoyalties14d  STRING
    ,unitsSoldSameSku1d  STRING
    ,campaignStatus  STRING
    ,advertisedSku  STRING
    ,salesOtherSku7d  STRING
    ,purchasesSameSku7d  STRING
    ,campaignBudgetAmount  STRING
    ,purchases7d  STRING
    ,unitsSoldSameSku30d  STRING
    ,costPerClick  STRING
    ,unitsSoldClicks14d  STRING
    ,adGroupName  STRING
    ,campaignId  STRING
    ,clickThroughRate  STRING
    ,kindleEditionNormalizedPagesRead14d  STRING
    ,acosClicks14d  STRING
    ,unitsSoldClicks30d  STRING
    ,portfolioId  STRING
    ,adId  STRING
    ,campaignBudgetCurrencyCode  STRING
    ,roasClicks7d  STRING
    ,unitsSoldSameSku14d  STRING
    ,unitsSoldClicks7d  STRING
    ,attributedSalesSameSku7d  STRING
    ,sales1d  STRING
    ,adGroupId  STRING
    ,purchasesSameSku14d  STRING
    ,unitsSoldOtherSku7d  STRING
    ,spend  STRING
    ,purchasesSameSku1d  STRING
    ,campaignBudgetType  STRING
    ,advertisedAsin  STRING
    ,purchases1d  STRING
    ,unitsSoldSameSku7d  STRING
    ,cost  STRING
    ,sales14d  STRING
    ,acosClicks7d  STRING
    ,sales30d  STRING
    ,impressions  STRING
    ,purchasesSameSku30d  STRING
    ,purchases14d  STRING
    ,purchases30d  STRING
    ,clicks  STRING
    ,campaignName STRING )
    STORED AS ALIORC
    LIFECYCLE 7;



drop table if EXISTS  whde.odps_amazon_adv_table3;
CREATE TABLE IF NOT EXISTS whde.odps_amazon_adv_table3(
                                                          date STRING
    ,purchases7d STRING
    ,cost STRING
    ,purchases30d STRING
    ,campaignId STRING
    ,clicks STRING
    ,purchases1d STRING
    ,impressions STRING
    ,adGroupId STRING
    ,purchases14d STRING
)
    STORED AS ALIORC
    LIFECYCLE 365;



drop table if EXISTS  whde.odps_amazon_adv_table4;
CREATE TABLE IF NOT EXISTS whde.odps_amazon_adv_table4(
                                                          date STRING
    ,attributedSalesSameSku1d STRING
    ,campaignBiddingStrategy STRING
    ,unitsSoldClicks1d STRING
    ,attributedSalesSameSku7d STRING
    ,placementClassification STRING
    ,attributedSalesSameSku14d STRING
    ,sales1d STRING
    ,sales7d STRING
    ,attributedSalesSameSku30d STRING
    ,kindleEditionNormalizedPagesRoyalties14d STRING
    ,purchasesSameSku14d STRING
    ,spend STRING
    ,purchasesSameSku1d STRING
    ,unitsSoldSameSku1d STRING
    ,purchases1d STRING
    ,purchasesSameSku7d STRING
    ,unitsSoldSameSku7d STRING
    ,purchases7d STRING
    ,unitsSoldSameSku30d STRING
    ,cost STRING
    ,costPerClick STRING
    ,unitsSoldClicks14d STRING
    ,sales14d STRING
    ,clickThroughRate STRING
    ,sales30d STRING
    ,impressions STRING
    ,kindleEditionNormalizedPagesRead14d STRING
    ,purchasesSameSku30d STRING
    ,purchases14d STRING
    ,unitsSoldClicks30d STRING
    ,purchases30d STRING
    ,clicks STRING
    ,unitsSoldSameSku14d STRING
    ,unitsSoldClicks7d STRING
)
    STORED AS ALIORC
    LIFECYCLE 365;


drop table if EXISTS  whde.odps_amazon_adv_table1;
CREATE TABLE IF NOT EXISTS whde.odps_amazon_adv_table1(
                                                          date  STRING
    ,attributedSalesSameSku1d  STRING
    ,campaignBiddingStrategy  STRING
    ,unitsSoldClicks1d  STRING
    ,attributedSalesSameSku7d  STRING
    ,topOfSearchImpressionShare  STRING
    ,attributedSalesSameSku14d  STRING
    ,sales1d  STRING
    ,sales7d  STRING
    ,campaignRuleBasedBudgetAmount  STRING
    ,attributedSalesSameSku30d  STRING
    ,kindleEditionNormalizedPagesRoyalties14d  STRING
    ,purchasesSameSku14d  STRING
    ,spend  STRING
    ,purchasesSameSku1d  STRING
    ,campaignBudgetType  STRING
    ,unitsSoldSameSku1d  STRING
    ,campaignStatus  STRING
    ,purchases1d  STRING
    ,purchasesSameSku7d  STRING
    ,unitsSoldSameSku7d  STRING
    ,campaignBudgetAmount  STRING
    ,purchases7d  STRING
    ,unitsSoldSameSku30d  STRING
    ,cost  STRING
    ,costPerClick  STRING
    ,unitsSoldClicks14d  STRING
    ,campaignId  STRING
    ,sales14d  STRING
    ,clickThroughRate  STRING
    ,sales30d  STRING
    ,impressions  STRING
    ,kindleEditionNormalizedPagesRead14d  STRING
    ,campaignApplicableBudgetRuleName  STRING
    ,purchasesSameSku30d  STRING
    ,purchases14d  STRING
    ,unitsSoldClicks30d  STRING
    ,campaignBudgetCurrencyCode  STRING
    ,purchases30d  STRING
    ,campaignApplicableBudgetRuleId  STRING
    ,clicks  STRING
    ,campaignName  STRING
    ,unitsSoldSameSku14d  STRING
    ,unitsSoldClicks7  STRING
)
    STORED AS ALIORC
    LIFECYCLE 360;



drop table if EXISTS  whde.odps_amazon_adv_table5;
CREATE TABLE IF NOT EXISTS whde.odps_amazon_adv_table5(
                                                          date STRING
    ,attributedSalesSameSku1d STRING
    ,roasClicks14d STRING
    ,unitsSoldClicks1d STRING
    ,matchType STRING
    ,attributedSalesSameSku14d STRING
    ,sales7d STRING
    ,attributedSalesSameSku30d STRING
    ,kindleEditionNormalizedPagesRoyalties14d STRING
    ,unitsSoldSameSku1d STRING
    ,campaignStatus STRING
    ,keyword STRING
    ,salesOtherSku7d STRING
    ,purchasesSameSku7d STRING
    ,campaignBudgetAmount STRING
    ,purchases7d STRING
    ,unitsSoldSameSku30d STRING
    ,costPerClick STRING
    ,unitsSoldClicks14d STRING
    ,adGroupName STRING
    ,campaignId STRING
    ,clickThroughRate STRING
    ,kindleEditionNormalizedPagesRead14d STRING
    ,acosClicks14d STRING
    ,unitsSoldClicks30d STRING
    ,portfolioId STRING
    ,campaignBudgetCurrencyCode STRING
    ,roasClicks7d STRING
    ,unitsSoldSameSku14d STRING
    ,unitsSoldClicks7d STRING
    ,keywordId STRING
    ,attributedSalesSameSku7d STRING
    ,topOfSearchImpressionShare STRING
    ,sales1d STRING
    ,adGroupId STRING
    ,keywordBid STRING
    ,targeting STRING
    ,purchasesSameSku14d STRING
    ,unitsSoldOtherSku7d STRING
    ,purchasesSameSku1d STRING
    ,campaignBudgetType STRING
    ,adKeywordStatus STRING
    ,keywordType STRING
    ,purchases1d STRING
    ,unitsSoldSameSku7d STRING
    ,cost STRING
    ,sales14d STRING
    ,acosClicks7d STRING
    ,sales30d STRING
    ,impressions STRING
    ,purchasesSameSku30d STRING
    ,purchases14d STRING
    ,purchases30d STRING
    ,clicks STRING
    ,campaignName STRING
)
    STORED AS ALIORC
    LIFECYCLE 360;