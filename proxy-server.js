app.get("/enrollments", async (req, res) => {
  const email = req.query.email;
  if (!email)
    return res.status(400).json({ success: false, error: "Missing email" });

  try {
    // 1️⃣ Get subscriptions by email
    const subsResponse = await fetch(
      `https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?query=${encodeURIComponent(email)}`,
      {
        headers: {
          "X-Seal-Token": SEAL_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );

    const subsData = await subsResponse.json();
    const subs = subsData.payload?.subscriptions || [];
    const enrollments = [];
    const now = new Date();

    // 2️⃣ Loop through each subscription to get detailed info
    for (const sub of subs) {
      const detailResponse = await fetch(
        `https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${sub.id}`,
        {
          headers: {
            "X-Seal-Token": SEAL_TOKEN,
            "Content-Type": "application/json",
          },
        }
      );

      if (!detailResponse.ok) {
        const errorText = await detailResponse.text();
        console.error(`Seal detail fetch failed for ${sub.id}:`, errorText);
        continue; // skip this one
      }

      const detailData = await detailResponse.json();
      const detail = detailData.payload;

      if (!detail.items || detail.items.length === 0) continue;
      const item = detail.items[0];
      const props = item.properties || [];
      const getProp = (key) => props.find((p) => p.key === key)?.value || "";

      const billingAttempts = detail.billing_attempts || [];
      const nextAttempt = billingAttempts.find(a => new Date(a.date) >= now) || null;

      // Prepare previous 4 payments (history)
      const previousPayments = billingAttempts
        .filter(a => new Date(a.date) < now)
        .slice(-4) // last 4
        .map(a => ({
          date: a.date,
          amount: item.price ? `$${item.price}` : "",
          status: a.status || "unknown"
        }));

      enrollments.push({
        subscription_id: sub.id,
        child_first_name: getProp("Child First Name"),
        child_last_name: getProp("Child Last Name"),
        cricclub_id: getProp("Child CricClub ID"),
        program: getProp("Program Level") || item.title || "",
        payment_frequency: getProp("Billing Interval") || sub.billing_interval || "",
        next_payment_date: nextAttempt?.date || "",
        next_payment_amount: item.price ? `$${item.price}` : "",
        parent_email: email,
        previous_payments: previousPayments
      });
    }

    // 3️⃣ Return final result
    res.json({ success: true, enrollments });

  } catch (err) {
    console.error("Error fetching enrollments:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});
