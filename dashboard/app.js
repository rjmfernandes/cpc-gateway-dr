const REFRESH_INTERVAL = 1000;


/*
 * Update a Kafka cluster card.
 */
function updateCluster(boxId, statusId, status) {

    const box = document.getElementById(boxId);
    const statusElement = document.getElementById(statusId);

    box.classList.remove(
        "up",
        "down",
        "unknown"
    );

    statusElement.classList.remove(
        "online",
        "down",
        "unknown"
    );


    if (status === "UP") {

        box.classList.add("up");
        statusElement.classList.add("online");

        statusElement.innerHTML =
            '<span class="status-dot"></span>ONLINE';

    } else if (status === "DOWN") {

        box.classList.add("down");
        statusElement.classList.add("down");

        statusElement.innerHTML =
            '<span class="status-dot"></span>FAILED';

    } else {

        box.classList.add("unknown");
        statusElement.classList.add("unknown");

        statusElement.innerHTML =
            '<span class="status-dot"></span>UNKNOWN';
    }
}


/*
 * Update Gateway status.
 */
function updateGateway(status) {

    const gateway =
        document.getElementById("gateway");

    const statusElement =
        document.getElementById("gateway-status");


    gateway.classList.remove(
        "up",
        "down",
        "unknown"
    );

    statusElement.classList.remove(
        "online",
        "down",
        "unknown"
    );


    if (status === "UP") {

        gateway.classList.add("up");
        statusElement.classList.add("online");

        statusElement.innerHTML =
            '<span class="status-dot"></span>ONLINE';

    } else if (status === "DOWN") {

        gateway.classList.add("down");
        statusElement.classList.add("down");

        statusElement.innerHTML =
            '<span class="status-dot"></span>DOWN';

    } else {

        gateway.classList.add("unknown");
        statusElement.classList.add("unknown");

        statusElement.innerHTML =
            '<span class="status-dot"></span>UNKNOWN';
    }
}


/*
 * Highlight which Kafka cluster the Gateway
 * is currently routing traffic toward.
 */
function updateRoute(route) {

    const routeElement =
        document.getElementById("gateway-route");

    const leftBranch =
        document.getElementById("route-left");

    const rightBranch =
        document.getElementById("route-right");


    routeElement.innerText = route;


    leftBranch.classList.remove("active");
    rightBranch.classList.remove("active");


    if (route === "kafka1-domain") {

        leftBranch.classList.add("active");

    } else if (route === "kafka2-domain") {

        rightBranch.classList.add("active");
    }
}


/*
 * Update Cluster Linking state.
 */
function updateMirrorState(state) {

    const element =
        document.getElementById("mirror-state");

    element.innerText =
        "State: " + state;

    element.classList.remove(
        "active",
        "inactive"
    );


    if (
        state === "ACTIVE" ||
        state === "MIRRORING"
    ) {

        element.classList.add("active");

    } else {

        element.classList.add("inactive");
    }
}


/*
 * Retrieve the current demo state.
 */
async function refresh() {

    try {

        const response = await fetch(
            "/api/status",
            {
                cache: "no-store"
            }
        );


        if (!response.ok) {

            throw new Error(
                "HTTP status " + response.status
            );
        }


        const data =
            await response.json();


        updateCluster(
            "kafka1",
            "kafka1-status",
            data.kafka1
        );


        updateCluster(
            "kafka2",
            "kafka2-status",
            data.kafka2
        );


        updateGateway(
            data.gateway
        );


        updateRoute(
            data.gatewayRoute
        );


        updateMirrorState(
            data.mirrorState
        );


    } catch (error) {

        console.error(
            "Could not retrieve dashboard status:",
            error
        );


        updateCluster(
            "kafka1",
            "kafka1-status",
            "UNKNOWN"
        );


        updateCluster(
            "kafka2",
            "kafka2-status",
            "UNKNOWN"
        );


        updateGateway(
            "UNKNOWN"
        );


        updateRoute(
            "unknown"
        );


        updateMirrorState(
            "UNKNOWN"
        );
    }
}


/*
 * Initial refresh and automatic polling.
 */
refresh();

setInterval(
    refresh,
    REFRESH_INTERVAL
);