package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
	"github.com/go-gota/gota/dataframe"

	"gopkg.in/ini.v1"

	"Simulations_v5/simulation"
)

type InvalidDataError struct {
	Asset    string
	Received int
	Expected int
}

func (e *InvalidDataError) Error() string {
	return fmt.Sprintf("[%s] Invalid Data: Expected [%d] Received [%d]", e.Asset, e.Expected, e.Received)
}

var EMAValues []int
var ReinvestPercentageValues []float64
var MinReturnValues []float64
var PercentDrop []float64
var BalanceTripwires []float64
var Strategies []string
var investmentAMT float64
var sellCondition int
var taxRate float64
var fees float64
var startDate string
var endDate string
var SimsComplete = 0
var numSims = 0
var WG sync.WaitGroup
var outFileMut sync.Mutex
var dataFileMut sync.Mutex
var bar pb.ProgressBar

func main() {
	color.Green("Running Simulations")
	confFile := "/mnt/glados/Programming/Go/Simulations_v5/conf/conf.ini"
	err := getParameters(confFile)
	if err != nil {
		log.Fatal(err)
	}
	logDir, err := getLogDir(confFile)
	if err != nil {
		log.Fatal(err)
	}
	outputDir, err := getOutputDir(confFile)
	if err != nil {
		log.Fatal(err)
	}
	dataDir, err := getDataDir(confFile)
	if err != nil {
		log.Fatal(err)
	}
	startDate, endDate, err := getDates(confFile)
	if err != nil {
		log.Fatal(err)
	}
	investmentAMT, taxRate, fees, err = getSimulationParams(confFile)
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	outFileName := fmt.Sprintf("%d%s%d_%d%d", start.Day(), start.Month(), start.Year(), start.Hour(), start.Minute())
	assets, err := getAssets(confFile)
	if err != nil {
		log.Fatal(err)
	}
	numSims = getNumSims(assets)
	WG.Add(numSims)
	bar = *pb.New(numSims)
	bar.Start()
	for _, asset := range assets {
		for _, strat := range Strategies {
			for _, ema := range EMAValues {
				for _, reinvestPerc := range ReinvestPercentageValues {
					for _, minReturn := range MinReturnValues {
						for _, percentDrop := range PercentDrop {
							for _, balanceTrip := range BalanceTripwires {
								logFile := fmt.Sprintf("%s/%s-%s/%s/%s/EMA-%v/", logDir, startDate, endDate, asset, strat, ema)
								if _, err := os.Stat(logFile); os.IsNotExist(err) {
									err := os.MkdirAll(logFile, 0755)
									if err != nil {
										log.Fatal(err)
									}
								}
								logFile = fmt.Sprintf("%s/MPBR-%v_%v_%v_%v.log", logFile, minReturn, percentDrop, balanceTrip, reinvestPerc)
								// fmt.Printf("%s/MPBR-%v_%v_%v_%v.log", logFile, minReturn, percentDrop, balanceTrip, reinvestPerc)
								go simulate(asset, strat, ema, reinvestPerc, minReturn, percentDrop, balanceTrip, logFile, dataDir, outputDir, outFileName)
							}
						}
					}
				}
			}
		}
	}
	WG.Wait()
	bar.Finish()
	s := fmt.Sprintf("Done in %v\nRaw results can be found in %s", time.Since(start), outputDir)
	color.Cyan(s)
}

func simulate(asset string, strat string, ema int, reinvestPerc float64, minReturn float64, percentDrop float64, balanceTrip float64, logFile string, dataDir string, outputDir string, outFileName string) {
	data, dataFile, err := getData(asset, dataDir)
	if err != nil {
		fmt.Println(err)
		var invalidDataError *InvalidDataError
		if errors.As(err, &invalidDataError) {
			bar.Increment()
			WG.Done()
			// log.Println(err)
			return
		} else {
			log.Fatal(err)
		}
	}
	sim, err := simulation.NewSimulation(asset, investmentAMT, taxRate, fees, dataFile, data, logFile)
	if err != nil {
		fmt.Println("hit new sim")
		log.Fatal(err)
	}
	err = simulation.SetStratParams(&sim, strat, sellCondition, ema, reinvestPerc, minReturn, percentDrop, balanceTrip)
	if err != nil {
		fmt.Println("hit set strat")
		log.Fatal(err)
	}
	r := simulation.RunSimulation(&sim)
	logResult(r, outputDir, outFileName)
	WG.Done()
}

func getDates(confFile string) (string, string, error) {
	cfg, err := ini.Load(confFile)
	if err != nil {
		return "", "", err
	}
	startDate := cfg.Section("Simulation").Key("start_date").String()
	endDate := cfg.Section("Simulation").Key("end_date").String()
	return startDate, endDate, nil
}
func getOutputDir(confFile string) (string, error) {
	cfg, err := ini.Load(confFile)
	if err != nil {
		return "", err
	}
	outputDir := cfg.Section("Files").Key("output_dir").String()
	return outputDir, nil
}

func getLogDir(confFile string) (string, error) {
	cfg, err := ini.Load(confFile)
	if err != nil {
		return "", err
	}
	logDir := cfg.Section("Files").Key("log_dir").String()
	return logDir, nil
}

func getDataDir(confFile string) (string, error) {
	cfg, err := ini.Load(confFile)
	if err != nil {
		fmt.Printf("Failed to read %v\n", err)
		return "", err
	}
	dataDir := cfg.Section("Files").Key("data_dir").String()
	return dataDir, nil
}

func getSimulationParams(confFile string) (float64, float64, float64, error) {
	cfg, err := ini.Load(confFile)
	if err != nil {
		fmt.Printf("Failed to read %v\n", err)
		return 0.0, 0.0, 0.0, err
	}
	investmentAMT, err := cfg.Section("Simulation").Key("invest_amt").Float64()
	if err != nil {
		fmt.Printf("Failed to read %v\n", err)
		return 0.0, 0.0, 0.0, err
	}
	taxRate, err := cfg.Section("Simulation").Key("tax_rate").Float64()
	if err != nil {
		fmt.Printf("Failed to read %v\n", err)
		return 0.0, 0.0, 0.0, err
	}
	fees, err := cfg.Section("Simulation").Key("fees").Float64()
	if err != nil {
		fmt.Printf("Failed to read %v\n", err)
		return 0.0, 0.0, 0.0, err
	}
	startDate = cfg.Section("Simulation").Key("start_date").String()
	endDate = cfg.Section("Simulation").Key("end_date").String()
	return investmentAMT, taxRate, fees, nil
}

func getMostRecentFile(dir string) (string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Printf("Failed to read %v\n", err)
		return "", err
	}
	var newestFile string
	var newestTime int64 = 0
	for _, f := range files {
		fi, err := os.Stat(dir + f.Name())
		if err != nil {
			fmt.Println(err)
		}
		currTime := fi.ModTime().Unix()
		if currTime > newestTime {
			newestTime = currTime
			newestFile = f.Name()
		}
	}
	return fmt.Sprintf("%s/%s", dir, newestFile), nil
}

func getData(asset string, dataDir string) (dataframe.DataFrame, string, error) {
	dataFileMut.Lock()
	defer dataFileMut.Unlock()
	assetDir := fmt.Sprintf("%s/%s/", dataDir, asset)
	dataFile, err := getMostRecentFile(assetDir)
	file, err := os.Open(dataFile)
	if err != nil {
		return dataframe.DataFrame{}, "", err
	}
	dataFrame := dataframe.ReadCSV(file)
	file.Close()
	// dataFrame = dataFrame.Drop(2)
	// dataFrame = dataFrame.Drop(2)
	// dataFrame = dataFrame.Drop(2)
	// dataFrame = dataFrame.Drop(3)
	dataFrame = dataFrame.Arrange(dataframe.Sort("Date"))
	start, err := time.Parse("02Jan2006", startDate)
	if err != nil {
		return dataframe.DataFrame{}, "", err
	}
	end, err := time.Parse("02Jan2006", endDate)
	if err != nil {
		return dataframe.DataFrame{}, "", err
	}
	delta := int((end.Unix() - start.Unix()) / 3600)
	if math.Abs(float64(dataFrame.Nrow()-delta)/24.0) > 600 {
		// fmt.Println("INVALID DATA")
		return dataframe.DataFrame{}, "", &InvalidDataError{asset, dataFrame.Nrow(), delta}
	}
	return dataFrame, dataFile, nil
}

func logResult(r simulation.Result, outputDir string, outFileName string) {
	outFileMut.Lock()
	defer outFileMut.Unlock()
	outDir := fmt.Sprintf("%s/%s", outputDir, r.AssetName)
	if _, err := os.Stat(outDir); os.IsNotExist(err) {
		err := os.MkdirAll(outDir, 0755)
		if err != nil {
			log.Fatal(err)
		}
	}
	fileName := fmt.Sprintf("%s/%s.csv", outDir, outFileName)
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.Write([]byte(r.ResultString)); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	f.Close()
	bar.Increment()
}

func getNumSims(assets []string) int {
	return len(assets) * len(EMAValues) * len(ReinvestPercentageValues) * len(MinReturnValues) * len(PercentDrop) * len(BalanceTripwires) * len(Strategies)
}

func getAssets(confFile string) ([]string, error) {
	cfg, err := ini.Load(confFile)
	if err != nil {
		fmt.Printf("Failed to read %v\n", err)
		return nil, err
	}
	assetsStr := cfg.Section("Simulation").Key("assets").String()
	assets := strings.Split(assetsStr, " ")
	return assets, nil
}

func getParameters(confFile string) error {
	cfg, err := ini.Load(confFile)
	if err != nil {
		fmt.Printf("Failed to read %v\n", err)
		return err
	}
	Strategies = cfg.Section("Parameters").Key("strategies").Strings(" ")
	if len(Strategies) < 1 {
		return errors.New("Config file not configured for [strategies]")
	}
	BalanceTripwires = cfg.Section("Parameters").Key("balance_tripwires").Float64s(" ")
	if len(BalanceTripwires) < 1 {
		return errors.New("Config file not configured for [balance_tripwires]")
	}
	PercentDrop = cfg.Section("Parameters").Key("percent_drops").Float64s(" ")
	if len(PercentDrop) < 1 {
		return errors.New("Config file not configured for [percent_drops]")
	}
	MinReturnValues = cfg.Section("Parameters").Key("min_returns").Float64s(" ")
	if len(MinReturnValues) < 1 {
		return errors.New("Config file not configured for [min_returns]")
	}
	ReinvestPercentageValues = cfg.Section("Parameters").Key("reinvest_percentages").Float64s(" ")
	if len(ReinvestPercentageValues) < 1 {
		return errors.New("Config file not configured for [reinvest_percentages]")
	}
	EMAValues = cfg.Section("Parameters").Key("ema_values").Ints(" ")
	if len(EMAValues) < 1 {
		return errors.New("Config file not configured for [ema_values]")
	}
	sellCondition, err = cfg.Section("Parameters").Key("sell_condition").Int()
	if err != nil {
		return errors.New("Config file not configured for [sell_condition]")
	}
	return nil
}
