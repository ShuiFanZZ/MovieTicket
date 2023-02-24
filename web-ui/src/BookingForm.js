import React, { useState } from 'react';

function BookingForm() {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [quantity, setQuantity] = useState(1);
  const [confirmation, setConfirmation] = useState(null);

  const handleSubmit = (e) => {
    e.preventDefault();

    // Generate confirmation message
    const message = `
      <h2>Thank you for booking a ticket!</h2>
      <p>Name: ${name}</p>
      <p>Email: ${email}</p>
      <p>Quantity: ${quantity}</p>
    `;

    // Display confirmation message
    setConfirmation(message);

    // Reset form
    setName('');
    setEmail('');
    setQuantity(1);
  };

  return (
    <div>
      <h1>Ticket Booking System</h1>
      <form onSubmit={handleSubmit}>
        <label htmlFor="name">Name:</label>
        <input type="text" id="name" value={name} onChange={(e) => setName(e.target.value)} required />
        <label htmlFor="email">Email:</label>
        <input type="email" id="email" value={email} onChange={(e) => setEmail(e.target.value)} required />

        <label htmlFor="quantity">Quantity:</label>
        <input type="number" id="quantity" value={quantity} onChange={(e) => setQuantity(e.target.value)} min="1" max="9" required />
        <button type="submit">Book Ticket</button>
      </form>
      {confirmation && <div dangerouslySetInnerHTML={{__html: confirmation}} />}
    </div>
  );
}

export default BookingForm;