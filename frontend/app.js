function createEvent(eventType, elementId, value) {
    const event = {
        event_type: eventType,
        element_id: elementId,
        value: value,
        timestamp: new Date().toISOString()
    };
    //console.log(event);
    fetch("/events", {
    method: "POST",
    headers: {
        "Content-Type": "application/json"
    },
    body: JSON.stringify(event)
});
}

    // =========================
    // Button
    // =========================
const buyButton = document.getElementById("buy-button");
buyButton.addEventListener("click", function () {
    createEvent(
        "button_click",
        "buy-button",
        null
    );
});


    // =========================
    // Checkbox
    // =========================
const notifications = document.getElementById("notifications");
notifications.addEventListener("change", function () {
    createEvent(
        "checkbox_change",
        "notifications",
        notifications.checked
    );
});


    // =========================
    // Slider
    // =========================
const volume = document.getElementById("volume");
volume.addEventListener("input", function () {
    createEvent(
        "slider_change",
        "volume",
        volume.value
    );
});


    // =========================
    // Text input
    // =========================
const username = document.getElementById("username");
username.addEventListener("input", function () {
    createEvent(
        "input_change",
        "username",
        username.value
    );
});


    // =========================
    // Radio buttons
    // =========================
const languages = document.querySelectorAll('input[name="language"]');
languages.forEach(function (radio) {
    radio.addEventListener("change", function () {
        createEvent(
            "radio_change",
            radio.name,
            radio.value
        );
    });
});

    // =========================
    // Dropdown
    // =========================

const country = document.getElementById("country");
country.addEventListener("change", function () {
        createEvent(
            "select_change",
            "country",
            country.value
        );
    });