sampledata = ARGV.shift || raise("sampledata file required")
count = ARGV.shift || raise("count required")

sampledata = File
  .readlines(sampledata)
  .map{|l| s, t = l.chomp.split(";"); [s, t.to_f]}

Integer(count).times do
  station, temp = *sampledata.sample
  temp = temp.to_i + rand(-10..10)
  dec = rand(0..9)
  puts "#{station};#{temp}.#{dec}"
end
