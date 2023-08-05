package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis"
	geo "github.com/oschwald/geoip2-golang"
)

const (
	siteOne   = "http://api.ipstack.com/"
	siteTwo   = "http://api.ipapi.com/api/"
	siteThree = "geosite.com"
	filename  = "count.txt"
)

var (
	keyOne  string = os.Getenv("StackAaccessKey")
	keyTwo  string = os.Getenv("APIAccessKey")
	country string
)

type Provider interface {
	GetCountry(ip net.IP) (string, error)
}

type IPStackAPIProvider struct {
	url       string
	accessKey string
	client    *http.Client
}

type ProviderSwitcher struct {
	sync.RWMutex
	currentProviderIndex int

	ProviderList []Provider
	count        uint32
	rediscache
}

type rediscache struct {
	redis *redis.Client
}

type GeoProvider struct {
	reader *geo.Reader
}

type Country struct {
	Name string `json:"country_name"`
}

func NewRedisCacheClient() (*rediscache, error) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	log.Println("redis connected")

	return &rediscache{
		redis: client,
	}, nil
}

func NewIPStackAPIProvider(url string, accessKey string) *IPStackAPIProvider {
	return &IPStackAPIProvider{
		url:       url,
		accessKey: accessKey,
		client: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

func NewGeoProvider(fileName string) (*GeoProvider, error) {
	reader, err := geo.Open(fileName)
	if err != nil {
		return nil, err
	}

	return &GeoProvider{
		reader: reader,
	}, nil
}

func NewProviderSwitcher(currentProvider int, providers ...Provider) (*ProviderSwitcher, error) {
	client, err := NewRedisCacheClient()
	if err != nil {
		log.Fatal(err)
	}

	if len(providers) == 0 {
		return nil, errors.New("expected at least one provider")
	}

	return &ProviderSwitcher{
		currentProviderIndex: 0,
		ProviderList:         providers,
		count:                0,
		rediscache:           *client,
	}, nil
}

func (r *rediscache) GetCountry(ip net.IP) (string, error) {
	return r.redis.Get(ip.String()).Result()
}

func (r *rediscache) SetCountry(ip net.IP, country string) error {
	return r.redis.Set(ip.String(), country, 5*time.Minute).Err()
}

func getClientIP(r *http.Request) net.IP {
	IPAddress := r.Header.Get("X-Real-Ip")

	if IPAddress == "" {
		IPAddress += r.Header.Get("x-forwarded-for")
	}
	if IPAddress == "" {
		IPAddress += r.RemoteAddr
	}
	return net.ParseIP(IPAddress)
}

func (i IPStackAPIProvider) GetCountry(ip net.IP) (string, error) {
	var param = url.Values{
		"access_key": {i.accessKey},
		"fields":     {"country_name"},
	}.Encode()

	payLoad := fmt.Sprintf("%s%s?%s", i.url, ip, param)

	resp, err := i.client.Get(payLoad)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	c := Country{}
	if err := json.Unmarshal(bs, &c); err != nil {
		return "", err
	}
	return c.Name, nil
}

func (g GeoProvider) GetCountry(ip net.IP) (string, error) {
	country, err := g.reader.Country(ip)
	if err != nil {
		return "", err
	}
	return country.Country.Names["en"], nil
}

func (p *ProviderSwitcher) Get(ip net.IP) (string, error) {
	count := atomic.LoadUint32(&p.count)

	if count == 5 {
		count = 0

		p.Lock()
		if p.currentProviderIndex == len(p.ProviderList)-1 {
			p.currentProviderIndex = 0
		} else {
			p.currentProviderIndex++
		}
		p.Unlock()
	}

	p.RLock()
	provider := p.ProviderList[p.currentProviderIndex]
	p.RUnlock()

	atomic.AddUint32(&count, 1)
	atomic.StoreUint32(&p.count, count)

	country, err := p.rediscache.GetCountry(ip)
	if err != nil {
		country, err := provider.GetCountry(ip)
		if err != nil {
			return "", nil
		}
		p.rediscache.SetCountry(ip, country)
	}

	return country, nil
}

func (p *ProviderSwitcher) writeToFile(file string) error {
	if err := ioutil.WriteFile(file, []byte(strconv.Itoa(int(p.count))), 0666); err != nil {
		return err
	}
	return nil
}

func (p *ProviderSwitcher) readFromFile(filename string) (uint32, error) {
	bs, err := ioutil.ReadFile(filename)
	if err != nil {
		return 0, nil
	}

	count, err := strconv.Atoi(string(bs))
	if err != nil {
		return 0, err
	}

	return uint32(count), nil
}

func main() {
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ipStackProvider := NewIPStackAPIProvider(siteOne, keyOne)
	ipAPIProvider := NewIPStackAPIProvider(siteTwo, keyTwo)
	geoProvider, err := NewGeoProvider("GeoLite2-Country.mmdb")
	if err != nil {
		log.Fatal(err)
	}

	providerSwitcher, err := NewProviderSwitcher(0, ipStackProvider, ipAPIProvider, geoProvider)
	if err != nil {
		log.Fatal(err)
	}

	count, err := providerSwitcher.readFromFile(filename)
	if err != nil {
		log.Println(err)
	}

	providerSwitcher.count = count

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		country, err := providerSwitcher.Get(getClientIP(r))
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		fmt.Printf("Provider %d - %s\n", providerSwitcher.currentProviderIndex, country)
		w.Write([]byte(country))
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: nil,
	}

	go func() {
		if err = srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen:%+s\n", err)
		}
	}()

	log.Printf("server started")

	<-done

	log.Printf("server stopped")

	ctxShutDown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		if err := providerSwitcher.writeToFile(filename); err != nil {
			log.Println(err)
		}
		cancel()
	}()

	if err = srv.Shutdown(ctxShutDown); err != nil {
		log.Fatalf("server Shutdown Failed:%+s", err)
	}

	log.Printf("server exited properly")

}
