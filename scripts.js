document.addEventListener('DOMContentLoaded', function() {
    // Global Variables
    const itemSelectorContainer = document.getElementById('item-selector-container');
    const chartCanvas = document.getElementById('priceChart');
    const priceStatsDisplay = document.getElementById('priceStatsDisplay');
    const newSMAPeriodInput = document.getElementById('newSMAPeriod');
    const addSMAButton = document.getElementById('addSMAButton');
    const activeSMAListDiv = document.getElementById('activeSMAList');
    const sidebarSearchInput = document.getElementById('sidebarSearch');
    const chartDisplayTitleElement = document.getElementById('chartDisplayTitle');

    // Timeframe UI Elements
    const btn30D = document.getElementById('btn30D');
    const btn3M = document.getElementById('btn3M');
    const btn6M = document.getElementById('btn6M');
    const btn1Y = document.getElementById('btn1Y');
    const btnAll = document.getElementById('btnAll');
    const btnCustomTimeframe = document.getElementById('btnCustomTimeframe');
    const customDateControls = document.getElementById('customDateControls');
    const customStartDateInput = document.getElementById('customStartDate');
    const customEndDateInput = document.getElementById('customEndDate');
    const applyCustomTimeframeButton = document.getElementById('applyCustomTimeframe');

    let priceChart = null;
    let activeTimeframe = "All"; // "30D", "3M", "6M", "1Y", "All", "Custom"
    let originalPriceData = [];
    let originalLabels = [];
    let activeSMAPeriods = [7]; // Initialize with a default array *first*.
    console.log('Initial activeSMAPeriods:', JSON.stringify(activeSMAPeriods));

    const smaColors = [
        'rgb(255, 99, 132)',  // Pink/Red
        'rgb(54, 162, 235)',  // Blue
        'rgb(255, 206, 86)',  // Yellow
        'rgb(153, 102, 255)', // Purple
        'rgb(255, 159, 64)',  // Orange
        'rgb(46, 204, 113)'   // Green
    ];

    function loadSMAPeriodsFromStorage() {
        let periodsToUse = [7]; // Default value for this function's scope
        const storedSMAPeriods = localStorage.getItem('activeSMAPeriods');
        console.log('Stored SMA periods from localStorage:', storedSMAPeriods);

        if (storedSMAPeriods) {
            try {
                const parsedPeriods = JSON.parse(storedSMAPeriods);
                console.log('Parsed SMA periods:', JSON.stringify(parsedPeriods));
                // Validate: must be an array, and if not empty, all elements must be positive numbers.
                if (Array.isArray(parsedPeriods)) {
                    if (parsedPeriods.length === 0) {
                        periodsToUse = []; // Valid empty array
                    } else if (parsedPeriods.every(p => typeof p === 'number' && p > 0)) {
                        periodsToUse = parsedPeriods; // Valid non-empty array
                    } else {
                        console.warn('Stored SMA periods data is invalid (not all positive numbers). Using default and resetting storage.');
                        localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
                    }
                } else {
                    console.warn('Stored SMA periods data is not an array. Using default and resetting storage.');
                    localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
                }
            } catch (e) {
                console.error('Error parsing stored SMA periods:', e, '. Using default and resetting storage.');
                localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
            }
        } else {
            console.log('No SMA periods found in localStorage. Using default and saving to storage.');
            localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
        }
        console.log('loadSMAPeriodsFromStorage will return:', JSON.stringify(periodsToUse));
        return periodsToUse;
    }

    activeSMAPeriods = loadSMAPeriodsFromStorage();
    console.log('Active SMA Periods after storage load attempt:', JSON.stringify(activeSMAPeriods));

    function saveSMAPeriods() {
        localStorage.setItem('activeSMAPeriods', JSON.stringify(activeSMAPeriods));
    }

    function filterDataByTimeframe(labels, prices, timeframe, startDateStr, endDateStr) {
        if (!labels || !prices || labels.length === 0 || prices.length === 0) {
            return { filteredLabels: [], filteredPrices: [] };
        }

        if (timeframe === "All") {
            return { filteredLabels: labels.slice(), filteredPrices: prices.slice() };
        }

        const filteredLabels = [];
        const filteredPrices = [];

        // Ensure dates are sorted (originalLabels are assumed to be sorted)
        // Convert latest date string to Date object for calculation
        const latestDataDate = new Date(labels[labels.length - 1] + "T00:00:00Z"); // Use Z for UTC to be safe with date-only strings

        let startDate;
        let endDate = latestDataDate; // Default end date is the latest available date

        if (timeframe === "Custom") {
            if (!startDateStr || !endDateStr) { // Should be validated before calling
                return { filteredLabels: labels.slice(), filteredPrices: prices.slice() }; // Or handle error
            }
            startDate = new Date(startDateStr + "T00:00:00Z");
            endDate = new Date(endDateStr + "T23:59:59Z"); // Inclusive end date
        } else {
            startDate = new Date(latestDataDate);
            switch (timeframe) {
                case "30D":
                    startDate.setDate(startDate.getDate() - 30);
                    break;
                case "3M":
                    startDate.setMonth(startDate.getMonth() - 3);
                    break;
                case "6M":
                    startDate.setMonth(startDate.getMonth() - 6);
                    break;
                case "1Y":
                    startDate.setFullYear(startDate.getFullYear() - 1);
                    break;
                default: // Should not happen if logic is correct
                    return { filteredLabels: labels.slice(), filteredPrices: prices.slice() };
            }
        }
        // Normalize startDate to the beginning of its day for comparison
        startDate.setHours(0, 0, 0, 0);


        for (let i = 0; i < labels.length; i++) {
            const currentDate = new Date(labels[i] + "T00:00:00Z"); // Use Z for UTC
            if (currentDate >= startDate && currentDate <= endDate) {
                filteredLabels.push(labels[i]);
                filteredPrices.push(prices[i]);
            }
        }
        return { filteredLabels, filteredPrices };
    }

    function fetchItemData() {
        fetch('item_data.json')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(jsonData => {
                itemSelectorContainer.innerHTML = '';
                buildSidebarNavigation(jsonData, itemSelectorContainer, 0);
            })
            .catch(error => {
                console.error('Error fetching item_data.json:', error);
                itemSelectorContainer.innerHTML = '<p>Error loading item data.</p>';
            });
    }

    function buildSidebarNavigation(data, parentElement, level = 0) {
        parentElement.innerHTML = '';

        const ul = document.createElement('ul');
        ul.className = `list-level-${level}`;
        ul.style.listStyleType = 'none';
        ul.style.paddingLeft = level > 0 ? '20px' : '0';

        for (const key in data) {
            const listItem = document.createElement('li');
            listItem.className = 'nav-item';

            const textElement = document.createElement('span');
            textElement.style.cursor = 'pointer';
            listItem.appendChild(textElement);

            if (typeof data[key] === 'string') {
                listItem.classList.add('item-leaf');
                textElement.textContent = key;
                listItem.dataset.csvPath = data[key];
                textElement.addEventListener('click', function() {
                    loadItemData(data[key], key);

                    const currentlySelected = document.querySelector('.nav-item.selected-leaf');
                    if (currentlySelected) {
                        currentlySelected.classList.remove('selected-leaf');
                    }
                    listItem.classList.add('selected-leaf');
                });
            } else if (typeof data[key] === 'object') {
                listItem.classList.add('item-category');
                textElement.textContent = '► ' + key;

                const childListContainer = document.createElement('div');
                childListContainer.className = 'child-list-container';
                childListContainer.style.display = 'none';
                listItem.appendChild(childListContainer);

                textElement.addEventListener('click', function(event) {
                    const isCurrentlyCollapsed = childListContainer.style.display === 'none';
                    if (isCurrentlyCollapsed) {
                        childListContainer.style.display = 'block';
                        textElement.textContent = '▼ ' + key;
                        if (childListContainer.innerHTML.trim() === '') {
                            buildSidebarNavigation(data[key], childListContainer, level + 1);
                        }
                    } else {
                        childListContainer.style.display = 'none';
                        textElement.textContent = '► ' + key;
                    }
                    event.stopPropagation();
                });
            }
            ul.appendChild(listItem);
        }
        parentElement.appendChild(ul);
    }

    function updateActiveTimeframeButton(selectedButton) {
        const timeframeButtons = [btn30D, btn3M, btn6M, btn1Y, btnAll, btnCustomTimeframe];
        timeframeButtons.forEach(button => {
            if (button) { // Ensure button exists
                if (button === selectedButton) {
                    button.classList.add('active');
                } else {
                    button.classList.remove('active');
                }
            }
        });
    }


    function loadItemData(csvPath, itemName) {
        console.log("Loading data for:", csvPath, "Item Name:", itemName);

        // Reset timeframe to "All" when new item is loaded
        activeTimeframe = "All";
        if (customDateControls) customDateControls.style.display = 'none';
        updateActiveTimeframeButton(btnAll);

        fetch(csvPath)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status} for ${csvPath}`);
                }
                return response.text();
            })
            .then(csvData => {
                const lines = csvData.trim().split(/\r?\n/);
                if (lines.length <= 1) {
                    console.warn("CSV data has no data rows for", csvPath);
                    originalLabels = [];
                    originalPriceData = [];
                } else {
                    const dataRows = lines.slice(1);
                    const tempLabels = [];
                    const tempPriceData = [];
                    const priceIndex = 2;
                    const dateCreatedIndex = 5;

                    dataRows.forEach(row => {
                        const columns = row.split(',');
                        if (columns.length > Math.max(priceIndex, dateCreatedIndex)) {
                            const rawDate = columns[dateCreatedIndex];
                            tempLabels.push(rawDate.substring(0, 10));
                            const price = parseFloat(columns[priceIndex]);
                            tempPriceData.push(isNaN(price) ? null : price);
                        }
                    });
                    originalLabels = tempLabels;
                    originalPriceData = tempPriceData;
                }

                if (chartDisplayTitleElement && itemName) {
                    chartDisplayTitleElement.textContent = `${itemName} Price Over Time`;
                } else if (chartDisplayTitleElement) {
                    chartDisplayTitleElement.textContent = 'Price Over Time';
                }
                console.log('Loading item data. Current activeSMAPeriods before updateChartWithIndicators:', JSON.stringify(activeSMAPeriods));
                if (originalLabels.length === 0 && originalPriceData.length === 0) {
                    console.warn("No data parsed from CSV:", csvPath, "Clearing chart.");
                    if (priceChart) {
                        priceChart.data.labels = [];
                        priceChart.data.datasets = [];
                        priceChart.update();
                    } else {
                         initializeChart();
                    }
                     if (priceStatsDisplay) { priceStatsDisplay.innerHTML = '<p>Price statistics not available.</p>'; }
                } else {
                    updateChartWithIndicators();
                }
            })
            .catch(error => {
                console.error('Failed to load or process item data:', error);
                if (chartDisplayTitleElement) {
                    chartDisplayTitleElement.textContent = 'Failed to Load Item Data';
                }
                 originalLabels = [];
                 originalPriceData = [];
                 if (priceChart) {
                    priceChart.data.labels = [];
                    priceChart.data.datasets = [];
                    priceChart.update();
                 } else {
                    initializeChart();
                 }
                 if (priceStatsDisplay) { priceStatsDisplay.innerHTML = '<p>Price statistics not available.</p>'; }
            });
    }

    function calculateSMA(dates, prices, periodDays) {
        if (!dates || !prices || dates.length !== prices.length || periodDays <= 0) {
          return new Array(prices ? prices.length : 0).fill(null);
        }
        const smaValues = new Array(prices.length).fill(null);
        for (let i = 0; i < dates.length; i++) {
          const currentDateStr = dates[i];
          // Using 'Z' to ensure UTC parsing, consistent with filterDataByTimeframe
          const currentDate = new Date(currentDateStr + "T00:00:00Z");
          const startDate = new Date(currentDate);
          startDate.setDate(currentDate.getDate() - (periodDays - 1));
          let sum = 0;
          let count = 0;
          for (let j = i; j >= 0; j--) {
            const loopDateStr = dates[j];
            // Using 'Z' to ensure UTC parsing
            const loopDate = new Date(loopDateStr + "T00:00:00Z");
            if (loopDate >= startDate && loopDate <= currentDate) {
              if (prices[j] !== null && !isNaN(prices[j])) {
                sum += prices[j];
                count++;
              }
            } else if (loopDate < startDate) {
              break;
            }
          }
          if (count > 0) {
            smaValues[i] = sum / count;
          }
        }
        return smaValues;
    }

    function renderActiveSMAList() {
        if (!Array.isArray(activeSMAPeriods)) {
            console.error('CRITICAL: activeSMAPeriods is not an array in renderActiveSMAList! Reverting to default [7].');
            activeSMAPeriods = [7];
            saveSMAPeriods();
        }
        if (!activeSMAListDiv) return;
        activeSMAListDiv.innerHTML = '';
        if (activeSMAPeriods.length === 0) {
          activeSMAListDiv.textContent = 'No active SMAs.';
          return;
        }
        activeSMAPeriods.forEach(period => {
          const itemDiv = document.createElement('div');
          itemDiv.classList.add('sma-item');
          const periodText = document.createElement('span');
          periodText.textContent = `SMA(${period})`;
          const removeButton = document.createElement('button');
          removeButton.textContent = 'Remove';
          removeButton.dataset.period = period;
          removeButton.addEventListener('click', function() {
            const periodToRemove = parseInt(this.dataset.period, 10);
            activeSMAPeriods = activeSMAPeriods.filter(p => p !== periodToRemove);
            saveSMAPeriods();
            renderActiveSMAList();
            updateChartWithIndicators();
          });
          itemDiv.appendChild(periodText);
          itemDiv.appendChild(removeButton);
          activeSMAListDiv.appendChild(itemDiv);
        });
    }

    if (addSMAButton) {
        addSMAButton.addEventListener('click', function() {
            if (!newSMAPeriodInput) return;
            const newPeriod = parseInt(newSMAPeriodInput.value, 10);
            if (isNaN(newPeriod) || newPeriod < 2 || newPeriod > 200) {
              alert('Please enter a valid SMA period (e.g., 2-200 days).');
              return;
            }
            if (activeSMAPeriods.includes(newPeriod)) {
              alert(`SMA(${newPeriod}) is already active.`);
              return;
            }
            if (activeSMAPeriods.length >= smaColors.length) {
                alert(`Maximum of ${smaColors.length} SMAs can be added.`);
                return;
            }
            activeSMAPeriods.push(newPeriod);
            activeSMAPeriods.sort((a, b) => a - b);
            saveSMAPeriods();
            renderActiveSMAList();
            updateChartWithIndicators();
        });
    }

    function initializeChart() {
        if (!chartCanvas) {
            console.error("Chart canvas not found!");
            return;
        }
        console.log('Initializing chart. Current activeSMAPeriods:', JSON.stringify(activeSMAPeriods));
        const ctx = chartCanvas.getContext('2d');
        if (!ctx) {
            console.error("Failed to get 2D context from canvas!");
            return;
        }

        priceChart = new Chart(ctx, {
          type: 'line',
          data: {
            labels: [],
            datasets: []
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              y: {
                beginAtZero: false,
                ticks: { callback: function(value) { return value.toLocaleString(); } }
              },
              x: {
                type: 'time',
                time: {
                    unit: 'day',
                    tooltipFormat: 'MMM d, yyyy',
                    displayFormats: { day: 'MMM d' }
                },
                ticks: { autoSkip: true, maxTicksLimit: 20 }
              }
            },
            plugins: {
              tooltip: {
                callbacks: {
                  label: function(context) {
                    let label = context.dataset.label || '';
                    if (label) { label += ': '; }
                    if (context.parsed.y !== null) { label += context.parsed.y.toLocaleString(); }
                    return label;
                  }
                }
              },
              legend: {
                position: 'top',
              }
            }
          }
        });
        renderActiveSMAList();
    }

    function updateChartWithIndicators() {
        if (!Array.isArray(activeSMAPeriods)) {
            console.error('CRITICAL: activeSMAPeriods is not an array in updateChartWithIndicators! Reverting to default [7].');
            activeSMAPeriods = [7];
            saveSMAPeriods();
        }
        if (!priceChart) {
            initializeChart();
            if (!priceChart) return;
        }

        // Get custom dates if 'Custom' is active, otherwise they can be null/undefined
        const customStart = (activeTimeframe === 'Custom' && customStartDateInput) ? customStartDateInput.value : null;
        const customEnd = (activeTimeframe === 'Custom' && customEndDateInput) ? customEndDateInput.value : null;

        const { filteredLabels, filteredPrices } = filterDataByTimeframe(
            originalLabels,
            originalPriceData,
            activeTimeframe,
            customStart,
            customEnd
        );

        priceChart.data.labels = filteredLabels; // Use filteredLabels
        const datasets = [];

        if (filteredPrices.length > 0) {
            datasets.push({
              label: 'Item Price',
              data: filteredPrices, // Use filteredPrices
              borderColor: 'rgb(0, 0, 0)',
              backgroundColor: 'rgba(0, 0, 0, 0.1)',
              tension: 0.1,
              borderWidth: 2,
              pointRadius: 2,
              pointHoverRadius: 4,
              order: 0
            });

            activeSMAPeriods.forEach((period, index) => {
                // Pass filteredLabels and filteredPrices to calculateSMA
                const smaData = calculateSMA(filteredLabels, filteredPrices, period);
                const color = smaColors[index % smaColors.length];

                datasets.push({
                    label: `SMA(${period})`,
                    data: smaData,
                    borderColor: color,
                    fill: false,
                    tension: 0.1,
                    borderWidth: 1.5,
                    pointRadius: 0,
                    pointHoverRadius: 0,
                    order: index + 1
                });
            });
        } else {
            datasets.push({
                label: 'Item Price',
                data: [],
                borderColor: 'rgb(0,0,0)'
            });
        }

        priceChart.data.datasets = datasets;
        priceChart.update();

        // Calculate and display price statistics
        if (priceStatsDisplay) {
            const startPrice = filteredPrices.length > 0 ? filteredPrices[0] : null;
            const currentPrice = filteredPrices.length > 0 ? filteredPrices[filteredPrices.length - 1] : null;
            const highestPrice = filteredPrices.length > 0 ? Math.max(...filteredPrices.filter(p => p !== null)) : null;
            const lowestPrice = filteredPrices.length > 0 ? Math.min(...filteredPrices.filter(p => p !== null)) : null;

            let statsHTML = '<p>No data available for the selected period.</p>';

            if (startPrice !== null && filteredPrices.length > 0) {
                const currentPricePercentChange = startPrice !== 0 ? ((currentPrice - startPrice) / startPrice) * 100 : null;
                const highestPricePercentChange = startPrice !== 0 ? ((highestPrice - startPrice) / startPrice) * 100 : null;
                const lowestPricePercentChange = startPrice !== 0 ? ((lowestPrice - startPrice) / startPrice) * 100 : null;

                function formatPrice(value) { return value !== null ? value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : 'N/A'; }
                function formatPercent(value) {
                    if (value === null || isNaN(value)) {
                        return 'N/A';
                    }
                    const numValue = parseFloat(value); // Ensure it's a number
                    let color = 'black'; // Default color
                    if (numValue > 0) {
                        color = 'green';
                    } else if (numValue < 0) {
                        color = 'red';
                    }
                    return `<span style="color: ${color};">${numValue.toFixed(2)}%</span>`;
                }

                statsHTML = `
                    <p><strong>Current Price:</strong> ${formatPrice(currentPrice)} (${formatPercent(currentPricePercentChange)})</p>
                    <p><strong>Highest Price (Period):</strong> ${formatPrice(highestPrice)} (${formatPercent(highestPricePercentChange)})</p>
                    <p><strong>Lowest Price (Period):</strong> ${formatPrice(lowestPrice)} (${formatPercent(lowestPricePercentChange)})</p>
                `;
            }
            priceStatsDisplay.innerHTML = statsHTML;
        }
    }

    // Initial calls
    fetchItemData();
    initializeChart();

    // Initialize active button (btnAll should have 'active' class from HTML)
    // updateActiveTimeframeButton(btnAll); // Already set in HTML, but good for consistency if HTML changes

    if (chartDisplayTitleElement) {
        chartDisplayTitleElement.textContent = 'Select an Item to View Data';
    }

    // --- Timeframe Button Event Listeners ---
    const predefinedTimeframeButtons = [
        { el: btn30D, timeframe: "30D" },
        { el: btn3M, timeframe: "3M" },
        { el: btn6M, timeframe: "6M" },
        { el: btn1Y, timeframe: "1Y" },
        { el: btnAll, timeframe: "All" }
    ];

    predefinedTimeframeButtons.forEach(item => {
        if (item.el) {
            item.el.addEventListener('click', function() {
                activeTimeframe = item.timeframe;
                updateActiveTimeframeButton(item.el);
                if (customDateControls) customDateControls.style.display = 'none';
                updateChartWithIndicators();
                console.log("Active timeframe set to: " + activeTimeframe);
            });
        }
    });

    if (btnCustomTimeframe) {
        btnCustomTimeframe.addEventListener('click', function() {
            activeTimeframe = "Custom";
            updateActiveTimeframeButton(btnCustomTimeframe);
            if (customDateControls) customDateControls.style.display = 'block';
            // Don't update chart until 'Apply' is clicked for custom dates
            console.log("Active timeframe set to: Custom. Custom controls shown.");
        });
    }

    if (applyCustomTimeframeButton) {
        applyCustomTimeframeButton.addEventListener('click', function() {
            const startDate = customStartDateInput ? customStartDateInput.value : null;
            const endDate = customEndDateInput ? customEndDateInput.value : null;

            if (!startDate || !endDate) {
                alert("Please select both a start and end date for the custom timeframe.");
                return;
            }
            if (new Date(startDate) > new Date(endDate)) {
                alert("Start date cannot be after end date.");
                return;
            }
            activeTimeframe = "Custom"; // Should already be set if custom controls are visible
            updateChartWithIndicators();
            console.log(`Custom timeframe applied: ${startDate} to ${endDate}`);
        });
    }

    function recursiveFilter(ulElement, term) {
        if (!ulElement) return false;

        let anyChildMatch = false;
        const listItems = Array.from(ulElement.children).filter(child => child.tagName === 'LI');

        for (const listItem of listItems) {
            let currentItemMatch = false;
            const textElement = listItem.querySelector('span');
            let itemText = '';
            if (textElement) {
                itemText = textElement.textContent.replace(/^[►▼]\s*/, '').toLowerCase();
                if (itemText.includes(term)) {
                    currentItemMatch = true;
                }
            }

            const childListContainer = listItem.querySelector('.child-list-container');
            let hasMatchingChildren = false;

            if (childListContainer) {
                const childUl = childListContainer.querySelector('ul');
                if (childUl && childUl.children.length > 0) {
                    if (recursiveFilter(childUl, term)) {
                        hasMatchingChildren = true;
                    }
                }
                if (currentItemMatch || hasMatchingChildren) {
                    currentItemMatch = true;
                    childListContainer.style.display = 'block';
                    if (textElement) textElement.textContent = '▼ ' + textElement.textContent.replace(/^[►▼]\s*/, '');
                } else {
                    childListContainer.style.display = 'none';
                    if (textElement) textElement.textContent = '► ' + textElement.textContent.replace(/^[►▼]\s*/, '');
                }
            }

            if (currentItemMatch) {
                listItem.style.display = "";
                anyChildMatch = true;
            } else {
                listItem.style.display = "none";
            }
        }
        return anyChildMatch;
    }

    function resetFilters() {
        const rootList = itemSelectorContainer.querySelector('ul.list-level-0');
        if (!rootList) return;

        const allListItems = rootList.getElementsByTagName('li');
        for (const listItem of allListItems) {
            listItem.style.display = "";
            const textElement = listItem.querySelector('span');
            const childListContainer = listItem.querySelector('.child-list-container');
            if (childListContainer) {
                childListContainer.style.display = 'none';
                if (textElement) textElement.textContent = '► ' + textElement.textContent.replace(/^[►▼]\s*/, '');
            }
        }
    }

    function filterSidebar(searchTerm) {
        const term = searchTerm.toLowerCase();
        const rootList = itemSelectorContainer.querySelector('ul.list-level-0');

        if (!rootList) {
            console.log("Root list not found for filtering.");
            return;
        }

        if (term === "") {
            resetFilters();
            return;
        }
        recursiveFilter(rootList, term);
    }

    if (sidebarSearchInput) {
        sidebarSearchInput.addEventListener('input', function() {
            filterSidebar(this.value);
        });
    }

    console.log("scripts.js loaded and DOMContentLoaded event fired.");
});
