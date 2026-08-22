const RULES_VERSION = "crm-marketing-rules-20260823-01";

export const MARKETING_RULES = Object.freeze({
  repeatOpportunityDays: 150,
  dormantDays: 365,
  longDormantDays: 730,
  highRepeatShootCount: 3,
  completedStatusAllow: ["completed","complete","done","delivered","本納","本納品","本納品済","納品済","完了","撮影済"],
  excludedStatusFragments: ["cancel","キャンセル","取消","中止","draft","下書き","未定","pending","失敗","failed"],
  recommendationMap: {
    "お宮参り": { category: "Family撮影", timingMonths: 6, reason: "成長記録として次の家族撮影を提案しやすい" },
    "七五三": { category: "Family撮影", timingMonths: 12, reason: "節目撮影後の家族記念日提案に向いている" },
    "ファミリー": { category: "Family撮影", timingMonths: 12, reason: "年1回の家族記録として提案しやすい" },
    "Family": { category: "Family撮影", timingMonths: 12, reason: "年1回の家族記録として提案しやすい" },
    "バースデー": { category: "Birthday撮影", timingMonths: 12, reason: "次の誕生日に合わせて提案しやすい" },
    "ハーフBD": { category: "Birthday撮影", timingMonths: 6, reason: "1歳記念への自然な導線がある" },
    "お食い初め": { category: "Family撮影", timingMonths: 6, reason: "成長記録の継続提案に向いている" },
    "マタニティ": { category: "ニューボーン / お宮参り", timingMonths: 2, reason: "出産後の記念撮影候補として提案できる" },
    "ニューボーン": { category: "お宮参り / Family撮影", timingMonths: 4, reason: "月齢の節目撮影へつなげやすい" },
    "入学卒業": { category: "Family撮影", timingMonths: 12, reason: "年度ごとの成長記録として提案できる" },
    "成人": { category: "Family撮影", timingMonths: 12, reason: "家族写真や記念追加撮影へつなげやすい" },
    "ウェディング": { category: "Anniversary撮影", timingMonths: 12, reason: "記念日撮影へつなげやすい" }
  },
  segmentDescriptions: {
    PROSPECT: "LINE接続済みだが予約実績がない顧客",
    NEW_CUSTOMER: "撮影実績が1回の顧客",
    REPEAT_CUSTOMER: "撮影実績が2回以上の顧客",
    HIGH_REPEAT: "撮影実績が3回以上の高頻度顧客",
    DORMANT: "最終撮影から365日以上経過し、未来予約がない顧客",
    LONG_DORMANT: "最終撮影から730日以上経過し、未来予約がない顧客",
    REPEAT_OPPORTUNITY: "過去利用があり、未来予約がなく、再提案時期に入った顧客",
    LINE_CONNECTED: "formal LINE IDが登録されている顧客",
    IDENTITY_REVIEW_REQUIRED: "Customer IDまたはLINE IDの確認が必要な顧客"
  }
});

export function marketingRulesHealth() {
  return {
    marketing_rules_version: RULES_VERSION,
    marketing_rules_source: "src/crm-marketing-rules.mjs",
    marketing_rules_mutation: false,
    marketing_identity_matching: "customer_id_only",
    marketing_name_matching: false,
    marketing_line_send_enabled: false,
    repeat_opportunity_days: MARKETING_RULES.repeatOpportunityDays,
    dormant_days: MARKETING_RULES.dormantDays,
    long_dormant_days: MARKETING_RULES.longDormantDays,
    high_repeat_shoot_count: MARKETING_RULES.highRepeatShootCount
  };
}
