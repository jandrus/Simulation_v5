package simulation

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/go-gota/gota/dataframe"

	"Simulations_v5/strategy"
)

type Result struct {
	AssetName    string // Name of asset being simulated
	ResultString string // string containing relavent comma separated results
	Err          error  // Error if Error occurs
}

type df struct {
	index int
	data  dataframe.DataFrame
	nRows int
}

type Simulation struct {
	initialInvestment  float64    // Initial Investment amount in USD
	assetName          string     // Name of asset being simulated
	dataFile           string     // data file used for the simulation
	logFile            string     // log file used for the simulation
	feePercentage      float64    // Percentage paid in fees for trading
	taxRate            float64    // Percentage paid in TAX for trading
	capital            float64    // Amount of capital ready to be traded for asset (in USD)
	reserves           float64    // Amount of capital in reserves for future capital if price drops (in USD)
	lastBuyPrice       float64    // Last Price the asset was bought at
	revenue            float64    // Amount of revenue accrued (in USD)
	tax                float64    // Amount of taxes accrued (in USD)
	fees               float64    // Amount of fees accrued (in USD)
	asset              float64    // Amount of asset in posession ready to be sold (in asset)
	numTransactions    int        // Number of transactions completed for the simulation
	strat              string     // MACD, PSAR, or other...(program it later)
	sellCondition      int        // Integer representing sell parameters used to sell (1, 2, .. 5) See strategy.go
	stratEMA           int        // EMA used for strategy
	reinvestPercentage float64    // Percentage of profit that is reallocated to capital for further investments
	minReturn          float64    // Min return for sell "Profit Margin"
	percentDrop        float64    // Percentage of negative return where reserves are used to supplement capital
	balanceTrip        float64    // Tripwire for capital and reserves to be balanced -> capital, reserves = (capital + reserves) / 2
	minReserves        float64    // Minimum held in reserves (point where percentDrop is no longer in effect)
	buys               []int      // List containing indices of times the simulation BUYS the asset
	sells              []int      // List containing indices of times the simulation SELLS the asset
	balances           []int      // List containing indices of times the simulation BALANCES capital and reserves
	openReserves       []int      // List containing indices of times the simulation OPENS RESERVES for more capital
	purchaseHistory    []purchase // List containing history of purchases bought
	dFrame             df         // Custom dataframe used to iterate through data for simulation
}

type purchase struct {
	price  float64
	amount float64
	index  int
}

type purchaseHistory []purchase

func (p purchaseHistory) Len() int {
	return len(p)
}

func (p purchaseHistory) Less(i, j int) bool {
	return p[i].price < p[j].price
}

func (p purchaseHistory) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func logEvent(event string, s *Simulation) error {
	f, err := os.OpenFile(s.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(event)); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

func RunSimulation(s *Simulation) Result {
	for {
		buy, sell, openRes := calcPositions(s)
		if sell {
			sellAsset(s)
			if s.capital/s.reserves > s.balanceTrip {
				balanceFunds(s)
			}
		} else if buy {
			buyAsset(s)
		} else if openRes {
			openReserves(s)
		}
		s.dFrame.index += 1
		if s.dFrame.index >= s.dFrame.nRows {
			s.dFrame.index -= 1
			break
		}
	}
	price := s.dFrame.data.Subset(s.dFrame.index).Select("Close").Elem(0, 0).Float()
	return Result{s.assetName, getResultString(s, price), nil}
}

func calcPositions(s *Simulation) (bool, bool, bool) {
	buy := false
	sell := false
	openRes := false
	price := s.dFrame.data.Subset(s.dFrame.index).Select("Close").Elem(0, 0).Float()
	currentReturn := (price - s.lastBuyPrice) / s.lastBuyPrice
	if s.capital > 0.0 {
		row := s.dFrame.data.Subset(s.dFrame.index)
		buy = isBuy(s.strat, row)
	} else if s.asset > 0.0 {
		row := s.dFrame.data.Subset(s.dFrame.index)
		sell = isSell(s.minReturn, currentReturn, row, s.sellCondition)
	}
	if s.capital < 1.0 && currentReturn < s.percentDrop && s.reserves > s.minReserves {
		openRes = true
	}
	return buy, sell, openRes
}

func isSell(minReturn float64, currentReturn float64, row dataframe.DataFrame, sellCondition int) bool {
	return strategy.IsSell(minReturn, currentReturn, &row, sellCondition)
}

/*
Sell asset:

	Sell asset based on purchase history
	Update simulation parameters (tax, revenue, fees, etc.)
	Update purchase history
*/
func sellAsset(s *Simulation) {
	price := s.dFrame.data.Subset(s.dFrame.index).Select("Close").Elem(0, 0).Float()
	sort.Sort(purchaseHistory(s.purchaseHistory))
	ph := make([]purchase, len(s.purchaseHistory))
	copy(ph, s.purchaseHistory)
	for _, p := range ph {
		currentReturn := (price - p.price) / p.price
		if currentReturn > s.minReturn {
			usdValueSold := p.amount * price
			s.asset -= p.amount
			fee := usdValueSold * s.feePercentage
			gain := usdValueSold - (p.amount * p.price)
			tax := 0.0
			if gain > 0.0 {
				tax = gain * s.taxRate
			}
			reward := gain - fee - tax
			toCapital := reward * s.reinvestPercentage
			revenue := reward - toCapital
			s.capital += usdValueSold - gain + toCapital
			s.revenue += revenue
			s.tax += tax
			s.fees += fee
			s.purchaseHistory = s.purchaseHistory[1:]
			snapshot := getSnapshot(s)
			event := fmt.Sprintf("SELL,Amount %g,%s\n", p.amount, snapshot)
			logEvent(event, s)
		} else {
			break
		}
	}
	s.numTransactions += 1
	s.sells = append(s.sells, s.dFrame.index)
}

func balanceFunds(s *Simulation) {
	total := s.capital + s.reserves
	s.capital = total / 2
	s.reserves = total / 2
	s.balances = append(s.balances, s.dFrame.index)
	snapshot := getSnapshot(s)
	event := fmt.Sprintf("BAL,%s\n", snapshot)
	logEvent(event, s)
}

func openReserves(s *Simulation) {
	s.capital = s.reserves / 2
	s.reserves = s.reserves / 2
	s.openReserves = append(s.openReserves, s.dFrame.index)
	snapshot := getSnapshot(s)
	event := fmt.Sprintf("OR,%s\n", snapshot)
	logEvent(event, s)
}

func getSnapshot(s *Simulation) string {
	timestamp, _ := s.dFrame.data.Subset(s.dFrame.index).Select("Date").Elem(0, 0).Int()
	price := s.dFrame.data.Subset(s.dFrame.index).Select("Close").Elem(0, 0).Float()
	snap := fmt.Sprintf("Timestamp %d,", timestamp)
	snap += fmt.Sprintf("Index %d,", s.dFrame.index)
	snap += fmt.Sprintf("Price %g,", roundFloat(price, 2))
	snap += fmt.Sprintf("Capital %g,", roundFloat(s.capital, 2))
	snap += fmt.Sprintf("Asset %g,", s.asset)
	snap += fmt.Sprintf("Reserves %g,", roundFloat(s.reserves, 2))
	snap += fmt.Sprintf("Revenue %g,", roundFloat(s.revenue, 2))
	snap += fmt.Sprintf("Tax %g,", roundFloat(s.tax, 2))
	snap += fmt.Sprintf("Fees %g,", roundFloat(s.fees, 2))
	snap += fmt.Sprintf("PurchaseHist [")
	for _, purchase := range s.purchaseHistory {
		snap += fmt.Sprintf("(Amount:%g Price:%g Index:%d),", purchase.amount, purchase.price, purchase.index)
	}
	snap += fmt.Sprintf("],")
	row := s.dFrame.data.Subset(s.dFrame.index)
	snap += strategy.GetStratString(s.strat, &row)
	return snap
}

/*
Buy asset:

	calculate amount of asset puchased with fees
	track fees, asset, and purchaseHistory
	update vars
*/
func buyAsset(s *Simulation) {
	fee := s.capital * s.feePercentage
	capital := s.capital - fee
	price := s.dFrame.data.Subset(s.dFrame.index).Select("Close").Elem(0, 0).Float()
	amount := capital / price
	s.fees += fee
	s.capital = 0.0
	s.asset += amount
	p := purchase{price: price, amount: amount, index: s.dFrame.index}
	s.purchaseHistory = append([]purchase{p}, s.purchaseHistory...)
	s.lastBuyPrice = price
	s.numTransactions += 1
	s.buys = append(s.buys, s.dFrame.index)
	snapshot := getSnapshot(s)
	event := fmt.Sprintf("BUY,%s\n", snapshot)
	logEvent(event, s)
}

func getBuyHold(s *Simulation) float64 {
	// Returns: profit, tax, fees
	initialPrice := s.dFrame.data.Subset(0).Select("Close").Elem(0, 0).Float()
	finalPrice := s.dFrame.data.Subset(s.dFrame.nRows-1).Select("Close").Elem(0, 0).Float()
	feeBuy := s.initialInvestment * s.feePercentage
	capital := s.initialInvestment - feeBuy
	amount := capital / initialPrice
	usdValueSold := amount * finalPrice
	feeSell := usdValueSold * s.feePercentage
	gain := usdValueSold - s.initialInvestment
	if gain > 0 {
		tax := gain * s.taxRate
		return roundFloat(gain-tax-feeSell, 2)
	}
	return roundFloat(gain-feeSell, 2)
}

func getSimTotalValue(s *Simulation) float64 {
	value := getAssetValue(s)
	return roundFloat(value+s.capital+s.reserves, 2)
}

func getAssetValue(s *Simulation) float64 {
	finalPrice := s.dFrame.data.Subset(s.dFrame.index).Select("Close").Elem(0, 0).Float()
	return s.asset * finalPrice
}

func roundFloat(v float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(v*ratio) / ratio
}

func getResultString(s *Simulation, price float64) string {
	start, _ := s.dFrame.data.Subset(0).Select("Date").Elem(0, 0).Int()
	end, _ := s.dFrame.data.Subset(s.dFrame.nRows-1).Select("Date").Elem(0, 0).Int()
	str := fmt.Sprintf("%d,", start)
	str += fmt.Sprintf("%d,", end)
	str += fmt.Sprintf("%s,", s.strat)
	str += fmt.Sprintf("%d,", s.stratEMA)
	str += fmt.Sprintf("%g,", s.reinvestPercentage)
	str += fmt.Sprintf("%g,", s.minReturn)
	str += fmt.Sprintf("%g,", s.percentDrop)
	str += fmt.Sprintf("%g,", s.balanceTrip)
	str += fmt.Sprintf("%g,", getBuyHold(s))
	str += fmt.Sprintf("%g,", roundFloat(s.capital+s.reserves+(s.asset*price), 2))
	str += fmt.Sprintf("%g,", roundFloat(s.revenue, 2))
	str += fmt.Sprintf("%g,", roundFloat(s.tax, 2))
	str += fmt.Sprintf("%g,", roundFloat(s.fees, 2))
	str += fmt.Sprintf("%d,", s.numTransactions)
	str += fmt.Sprintf("[")
	for _, buy := range s.buys {
		str += fmt.Sprintf("%d ", buy)
	}
	str += fmt.Sprintf("],")
	str += fmt.Sprintf("[")
	for _, sell := range s.sells {
		str += fmt.Sprintf("%d ", sell)
	}
	str += fmt.Sprintf("],")
	str += fmt.Sprintf("[")
	for _, balance := range s.balances {
		str += fmt.Sprintf("%d ", balance)
	}
	str += fmt.Sprintf("],")
	str += fmt.Sprintf("[")
	for _, openR := range s.openReserves {
		str += fmt.Sprintf("%d ", openR)
	}
	str += fmt.Sprintf("],%s\n", s.dataFile)
	return str
}

func isBuy(strat string, row dataframe.DataFrame) bool {
	return strategy.IsBuy(strat, &row)
}

func newDF(dFrame dataframe.DataFrame) df {
	return df{0, dFrame, dFrame.Nrow()}
}

/*
Init for simulation
*/
func NewSimulation(asset string, investAMT float64, taxRate float64, feePercentage float64, dataFile string, data dataframe.DataFrame, logFile string) (Simulation, error) {
	dFrame := newDF(data)
	s := Simulation{
		initialInvestment:  investAMT,
		assetName:          asset,
		dataFile:           dataFile,
		logFile:            logFile,
		feePercentage:      feePercentage,
		taxRate:            taxRate,
		capital:            investAMT / 2,
		reserves:           investAMT / 2,
		lastBuyPrice:       0.0,
		revenue:            0.0,
		tax:                0.0,
		fees:               0.0,
		asset:              0.0,
		numTransactions:    0.0,
		strat:              "",
		stratEMA:           0,
		reinvestPercentage: 0.0,
		minReturn:          0.0,
		percentDrop:        0,
		balanceTrip:        0.0,
		minReserves:        investAMT * 0.125,
		buys:               []int{},
		sells:              []int{},
		balances:           []int{},
		openReserves:       []int{},
		purchaseHistory:    []purchase{},
		dFrame:             dFrame,
	}
	return s, nil
}

/*
Sets Statistical params for simulation
*/
func SetStratParams(s *Simulation, strat string, sellCondition int, ema int, reinvestPercentage float64, minReturn float64, percentDrop float64, balanceTrip float64) error {
	if strategy.IsValidStrategy(strat) {
		s.strat = strat
	} else {
		return errors.New("Invalid strategy")
	}
	s.stratEMA = ema
	s.sellCondition = sellCondition
	emaStr := fmt.Sprintf("EMA_%d", ema)
	s.dFrame.data = s.dFrame.data.Rename("EMA", emaStr)
	s.reinvestPercentage = reinvestPercentage
	s.minReturn = minReturn
	s.percentDrop = -1 * percentDrop
	s.balanceTrip = balanceTrip
	s.dFrame.index = 0
	return nil
}
