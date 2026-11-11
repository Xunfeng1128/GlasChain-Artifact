package broker

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"bufio"
	"log"
	"os"
)

type Broker struct {
	BrokerRawMegs  map[string]*message.BrokerRawMeg
	ChainConfig    *params.ChainConfig
	BrokerAddress  []string
	RawTx2BrokerTx map[string][]string
}

func (b *Broker) NewBroker(pcc *params.ChainConfig) {
	b.BrokerRawMegs = make(map[string]*message.BrokerRawMeg)
	b.RawTx2BrokerTx = make(map[string][]string)
	b.ChainConfig = pcc
	b.BrokerAddress = b.initBrokerAddr(params.BrokerNum)
}

func (b *Broker) IsBroker(address string) bool {
	for _, brokerAddress := range b.BrokerAddress {
		if brokerAddress == address {
			return true
		}
	}
	return false
}

func (b *Broker) initBrokerAddr(num int) []string {
	brokerAddress := make([]string, 0)
	if num <= 0 {
		return brokerAddress
	}
	filePath := params.BrokerFilePath
	readFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Error opening broker_300k_BlockTransaction file %s: %v", filePath, err)
	}
	defer readFile.Close()
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		brokerAddress = append(brokerAddress, fileScanner.Text())
		num--
		if num == 0 {
			break
		}
	}
	if err := fileScanner.Err(); err != nil {
		log.Fatalf("Error reading broker_300k_BlockTransaction file %s: %v", filePath, err)
	}
	return brokerAddress
}
