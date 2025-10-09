app.get("/enrollments", async (req, res) => {
  const email = req.query.email;
  if (!email) {
    return res.status(400).json({ success: false, error: "Missing email parameter" });
  }

  try {
    // Fetch subscriptions from Seal by email
    const subsResponse = await axios.get(
      `https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?query=${encodeURIComponent(email)}`,
      { headers: { "X-Seal-Token": process.env.SEAL_TOKEN } }
    );

    const subs = subsResponse.data.subscriptions || [];
    const enrollments = [];

    // For each subscription, get detailed info to extract child + payment data
    for (const sub of subs) {
      const detailResponse = await axios.get(
        `https://app.sealsubscriptions.com/shopify/merchant/api/subscription/${sub.id}`,
        { headers: { "X-Seal-Token": process.env.SEAL_TOKEN } }
      );
      const detail = detailResponse.data;

      if (!detail.items || detail.items.length === 0) continue;
      const item = detail.items[0];
      const props = item.properties || [];

      const getProp = (key) => props.find(p => p.key === key)?.value || "";

      const childFirstName = getProp("Child First Name");
      const childLastName = getProp("Child Last Name");
      const cricclubId = getProp("Child CricClub ID");
      const program = getProp("Program Level");
      const paymentFrequency = getProp("Billing Interval");

      const billingAttempts = detail.billing_attempts || [];
      const nextAttempt = billingAttempts.length ? billingAttempts[0] : null;
      const nextPaymentDate = nextAttempt ? nextAttempt.date : "";
      const nextPaymentAmount = item.price ? `$${item.price}` : "";

      enrollments.push({
        subscription_id: sub.id,
        child_first_name: childFirstName,
        child_last_name: childLastName,
        cricclub_id: cricclubId,
        program,
        payment_frequency: paymentFrequency,
        next_payment_date: nextPaymentDate,
        next_payment_amount: nextPaymentAmount,
        parent_email: email
      });
    }

    res.json({ success: true, enrollments });
  } catch (err) {
    console.error("Error fetching enrollments:", err.response?.data || err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});
