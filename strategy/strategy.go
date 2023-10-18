package strategy

import (
	"fmt"

	"github.com/go-gota/gota/dataframe"
)

/*
LOCAL VARS:
	strategies
PUBLIC FUNCTIONS:
	IsBuy
	IsValidStrategy
PRIVATE FUNCTIONS:
	isBuyMACD
	isBuyMACDCHAI
	isBuyPSAR
*/

var strategies = []string{"MACD-CHAI", "MACD", "PSAR", "MACD-PSAR", "alt-MACD"}

func IsBuy(strat string, dataFrame *dataframe.DataFrame) bool {
	switch strat {
	case "MACD-CHAI":
		return isBuyMACDCHAI(dataFrame)
	case "MACD-PSAR":
		return isBuyMACD(dataFrame) && isBuyPSAR(dataFrame)
	case "PSAR":
		return isBuyPSAR(dataFrame)
	case "MACD":
		return isBuyMACD(dataFrame)
	case "alt-MACD":
		return isBuyAltMACD(dataFrame)
	}
	return false
}

func IsSell(minReturn float64, currentReturn float64, dataFrame *dataframe.DataFrame, sellCondition int) bool {
	switch sellCondition {
	case 1:
		return sellCondition1(minReturn, currentReturn)
	case 2:
		price := dataFrame.Select("Close").Elem(0, 0).Float()
		ema := dataFrame.Select("EMA").Elem(0, 0).Float()
		return sellCondition2(price, ema, minReturn, currentReturn)
	case 3:
		price := dataFrame.Select("Close").Elem(0, 0).Float()
		ema := dataFrame.Select("EMA").Elem(0, 0).Float()
		return sellCondition3(price, ema, minReturn, currentReturn)
	case 4:
		price := dataFrame.Select("Close").Elem(0, 0).Float()
		ema := dataFrame.Select("EMA").Elem(0, 0).Float()
		macd := dataFrame.Select("MACD").Elem(0, 0).Float()
		signal := dataFrame.Select("SIGNAL").Elem(0, 0).Float()
		return sellCondition4(price, ema, macd, signal, minReturn, currentReturn)
	case 5:
		price := dataFrame.Select("Close").Elem(0, 0).Float()
		ema := dataFrame.Select("EMA").Elem(0, 0).Float()
		return sellCondition5(price, ema, minReturn, currentReturn)
	case 6:
		dP := dataFrame.Select("dP").Elem(0, 0).Float()
		return sellCondition6(dP, minReturn, currentReturn)
	}
	return false
}

func sellCondition6(dP float64, minReturn float64, currentReturn float64) bool {
	// dP < 0 && CR > MR
	if dP < 0.0 && currentReturn > minReturn {
		return true
	}
	return false
}

func sellCondition5(price float64, ema float64, minReturn float64, currentReturn float64) bool {
	// EMA < P && CR > MR
	if ema > price && currentReturn > minReturn {
		return true
	}
	return false
}

func sellCondition4(price float64, ema float64, macd float64, signal float64, minReturn float64, currentReturn float64) bool {
	// EMA > P && CR > 0 || dP < 0 && CR > 0
	if ema > price && macd < signal && currentReturn > minReturn {
		return true
	}
	return false
}

func sellCondition3(price float64, ema float64, minReturn float64, currentReturn float64) bool {
	// EMA < P && CR > MR
	if ema < price && currentReturn > minReturn {
		return true
	}
	return false
}

func sellCondition2(price float64, ema float64, minReturn float64, currentReturn float64) bool {
	// EMA > P && CR > MR || CR > 2*MR
	if ema > price && currentReturn > minReturn || currentReturn > 2*minReturn {
		return true
	}
	return false
}

func sellCondition1(minReturn float64, currentReturn float64) bool {
	// CR > MR
	if currentReturn > minReturn {
		return true
	}
	return false
}

func GetStratString(strat string, dataFrame *dataframe.DataFrame) string {
	str := fmt.Sprintf("Strat: [")
	switch strat {
	case "MACD-CHAI":
		ema := dataFrame.Select("EMA").Elem(0, 0).Float()
		macd := dataFrame.Select("MACD").Elem(0, 0).Float()
		signal := dataFrame.Select("SIGNAL").Elem(0, 0).Float()
		chai := dataFrame.Select("CHAI").Elem(0, 0).Float()
		str += fmt.Sprintf("EMA: %g,MACD: %g,SIG: %g,CHAI: %g]", ema, macd, signal, chai)
	case "MACD-PSAR":
		ema := dataFrame.Select("EMA").Elem(0, 0).Float()
		macd := dataFrame.Select("MACD").Elem(0, 0).Float()
		signal := dataFrame.Select("SIGNAL").Elem(0, 0).Float()
		sar := dataFrame.Select("SAR").Elem(0, 0).Float()
		str += fmt.Sprintf("EMA: %g,MACD: %g,SIG: %g,PSAR: %g]", ema, macd, signal, sar)
	case "PSAR":
		ema := dataFrame.Select("EMA").Elem(0, 0).Float()
		sar := dataFrame.Select("SAR").Elem(0, 0).Float()
		str += fmt.Sprintf("EMA: %g,PSAR: %g]", ema, sar)
	case "MACD":
		ema := dataFrame.Select("EMA").Elem(0, 0).Float()
		macd := dataFrame.Select("MACD").Elem(0, 0).Float()
		signal := dataFrame.Select("SIGNAL").Elem(0, 0).Float()
		str += fmt.Sprintf("EMA: %g,MACD: %g,SIG: %g]", ema, macd, signal)
	case "alt-MACD":
		ema := dataFrame.Select("EMA").Elem(0, 0).Float()
		macd := dataFrame.Select("MACD").Elem(0, 0).Float()
		signal := dataFrame.Select("SIGNAL").Elem(0, 0).Float()
		str += fmt.Sprintf("EMA: %g,MACD: %g,SIG: %g]", ema, macd, signal)
	}
	return str
}

func isBuyPSAR(dataFrame *dataframe.DataFrame) bool {
	price := dataFrame.Select("Close").Elem(0, 0).Float()
	ema := dataFrame.Select("EMA").Elem(0, 0).Float()
	sar := dataFrame.Select("SAR").Elem(0, 0).Float()
	return sar < price && price > ema
}

func isBuyMACD(dataFrame *dataframe.DataFrame) bool {
	price := dataFrame.Select("Close").Elem(0, 0).Float()
	ema := dataFrame.Select("EMA").Elem(0, 0).Float()
	macd := dataFrame.Select("MACD").Elem(0, 0).Float()
	signal := dataFrame.Select("SIGNAL").Elem(0, 0).Float()
	return macd > signal && price > ema
}

func isBuyAltMACD(dataFrame *dataframe.DataFrame) bool {
	price := dataFrame.Select("Close").Elem(0, 0).Float()
	ema := dataFrame.Select("EMA").Elem(0, 0).Float()
	macd := dataFrame.Select("MACD").Elem(0, 0).Float()
	signal := dataFrame.Select("SIGNAL").Elem(0, 0).Float()
	return macd > signal && price > ema && macd < 0
}

func isBuyMACDCHAI(dataFrame *dataframe.DataFrame) bool {
	price := dataFrame.Select("Close").Elem(0, 0).Float()
	ema := dataFrame.Select("EMA").Elem(0, 0).Float()
	macd := dataFrame.Select("MACD").Elem(0, 0).Float()
	signal := dataFrame.Select("SIGNAL").Elem(0, 0).Float()
	chai := dataFrame.Select("CHAI").Elem(0, 0).Float()
	return macd > signal && price > ema && chai > 0.0
}

func IsValidStrategy(strategy string) bool {
	for _, strat := range strategies {
		if strat == strategy {
			return true
		}
	}
	return false
}
