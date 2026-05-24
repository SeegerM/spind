db = db.getSiblingDB("shop");

db.reviews.insertMany([
  {
    review_id: 1,
    product_id: 10,
    customer_id: 1,
    rating: 5,
    text: "Great product"
  },
  {
    review_id: 2,
    product_id: 11,
    customer_id: 2,
    rating: 4,
    text: "Works fine"
  }
]);

db.sessions.insertMany([
  {
    session_id: "s1",
    customer_id: 1,
    device: "desktop",
    events: [
      { type: "view", product_id: 10 },
      { type: "cart", product_id: 10 }
    ]
  },
  {
    session_id: "s2",
    customer_id: 2,
    device: "mobile",
    events: [
      { type: "view", product_id: 11 }
    ]
  }
]);