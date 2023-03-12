import { render } from '@testing-library/react';
import React, { useState } from 'react';

function BookingForm() {
    const [name, setName] = useState('');
    const [email, setEmail] = useState('');
    const [quantity, setQuantity] = useState(1);
    const [confirmation, setConfirmation] = useState(null);
    const [activeView, setView] = useState(0);

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
        // setName('');
        // setEmail('');
        setQuantity(1);
        setView(1);
    };

    const handelCancel = () => {
        setView(0);
        console.log({ activeView });
    };

    const BookingView = () => {
        return (
            <form onSubmit={handleSubmit}>
                <label htmlFor="name">Name:</label>
                <input type="text" id="name" value={name} onChange={(e) => setName(e.target.value)} required />
                <label htmlFor="email">Email:</label>
                <input type="email" id="email" value={email} onChange={(e) => setEmail(e.target.value)} required />

                <label htmlFor="quantity">Quantity:</label>
                <input type="number" id="quantity" value={quantity} onChange={(e) => setQuantity(e.target.value)} min="1" max="9" required />
                <button type="submit">Book Ticket</button>
            </form>
        );
    };

    const ChooseSeatView = () => {
        return (
            <div id="theater">
                <h4>Please Choose Your Seats</h4>
                <h1>Screen</h1>
                <div type="seat-grid">
                    <div type="seat" id="0"></div>
                    <div type="seat" id="1"></div>
                    <div type="seat" id="2"></div>
                    <div type="seat" id="3"></div>
                    <div type="seat" id="4"></div>
                    <div type="seat" id="5"></div>
                    <div type="seat" id="6"></div>
                    <div type="seat" id="7"></div>
                    <div type="seat" id="8"></div>
                    <div type="seat" id="9"></div>
                    <div type="seat" id="10"></div>
                    <div type="seat" id="11"></div>
                    <div type="seat" id="12"></div>
                    <div type="seat" id="13"></div>
                    <div type="seat" id="14"></div>
                    <div type="seat" id="15"></div>
                    <div type="seat" id="16"></div>
                    <div type="seat" id="17"></div>
                    <div type="seat" id="18"></div>
                    <div type="seat" id="19"></div>
                </div>
                <div type="button_group">
                    <button type="button" id="cancel" onClick={() => handelCancel()}>Cacnel</button>
                </div>
                    
                
            </div>
        );

    };

    let book_view = activeView===0 && BookingView();
    let choose_seat_view = activeView===1 && ChooseSeatView();

    return (
        <div>
            <h1>Ticket Booking System</h1>
            {book_view}
            {choose_seat_view}
        </div>
    );
}

export default BookingForm;