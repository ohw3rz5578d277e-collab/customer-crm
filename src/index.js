export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/api/health") {
      return Response.json({
        ok: true,
        service: "customer-crm-api",
        time: new Date().toISOString()
      });
    }

    if (url.pathname === "/api/customers" && request.method === "GET") {
      try {
        const keyword = (url.searchParams.get("keyword") || "").trim();
        const limitRaw = parseInt(url.searchParams.get("limit") || "50", 10);
        const limit = Math.max(1, Math.min(limitRaw, 100));

        let result;

        if (keyword) {
          const like = "%" + keyword + "%";
          result = await env.DB.prepare(
            `
            SELECT
              customer_id,
              name,
              furigana,
              line_display_name,
              phone,
              email,
              last_shoot_date,
              repeat_count,
              repeat_count_1y,
              total_revenue,
              avg_order_value,
              acquisition_source,
              dormant_days,
              photo_public_ok,
              created_at,
              updated_at
            FROM customers
            WHERE
              name LIKE ?1
              OR furigana LIKE ?1
              OR line_display_name LIKE ?1
              OR phone LIKE ?1
              OR email LIKE ?1
            ORDER BY updated_at DESC
            LIMIT ?2
            `
          )
            .bind(like, limit)
            .all();
        } else {
          result = await env.DB.prepare(
            `
            SELECT
              customer_id,
              name,
              furigana,
              line_display_name,
              phone,
              email,
              last_shoot_date,
              repeat_count,
              repeat_count_1y,
              total_revenue,
              avg_order_value,
              acquisition_source,
              dormant_days,
              photo_public_ok,
              created_at,
              updated_at
            FROM customers
            ORDER BY updated_at DESC
            LIMIT ?1
            `
          )
            .bind(limit)
            .all();
        }

        return Response.json({
          ok: true,
          count: result.results ? result.results.length : 0,
          items: result.results || []
        });
      } catch (error) {
        return Response.json(
          {
            ok: false,
            message: error && error.message ? error.message : "Unknown error"
          },
          { status: 500 }
        );
      }
    }

    return Response.json(
      {
        ok: false,
        message: "Not Found"
      },
      { status: 404 }
    );
  }
};
