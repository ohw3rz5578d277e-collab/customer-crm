# Customer 360 UI contract

## Marketing HOME priority

Primary visual focus:

1. 今アプローチすべき顧客
2. 次の家族イベント
3. Supporting KPIs

## Customer list privacy

The list shows family count, next event, realized LTV, shoot count, last shoot, prefecture/city summary, LINE state, and recommendation.

It does not show full street address or exact child birthdates.

## Customer 360 detail order

1. 顧客サマリー
2. 家族情報
3. 次の撮影チャンス
4. 撮影履歴
5. 購入 / LTV
6. LINE / 接点
7. 流入元
8. マーケティング履歴
9. メモ

Exact family profile fields are visible only inside the Customer 360 detail surface.

## Opportunity semantics

Birthday, half birthday, first birthday, 七五三, school events, coming-of-age, and anniversary are derived candidates. Inferred school events and 七五三 are never persisted as confirmed facts by this foundation.

## LINE

The UI exposes draft wording only. There is no automatic LINE send action in this foundation.
