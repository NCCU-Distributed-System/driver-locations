let map = L.map("map").setView([25.03, 121.56], 12);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png").addTo(map);

let markers = {};

function updateMarkers(data) {
  data.forEach((driver) => {
    let id = driver.driver_id;
    let latlng = [driver.lat, driver.lon];

    if (markers[id]) {
      markers[id].setLatLng(latlng);
    } else {
      markers[id] = L.circleMarker(latlng, {
        radius: 8, // 圓形半徑（像素）
        color: "#003366", // 邊框顏色
        fillColor: "#0055aa", // 填滿顏色（深藍）
        fillOpacity: 0.9, // 填滿透明度
        weight: 1, // 邊框寬度
      })
        .addTo(map)
        .bindPopup("Driver " + id);
    }
  });
}

async function fetchAndUpdate() {
  const response = await fetch("/locations");
  const data = await response.json();
  updateMarkers(data);
}

setInterval(fetchAndUpdate, 1000);
